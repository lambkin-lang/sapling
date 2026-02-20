/*
 * sapling.c - copy-on-write B+ tree with MVCC and nested transactions
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================== */
/* Constants                                                            */
/* ================================================================== */

#define SAP_MAGIC    0x53415054U
#define SAP_VERSION  1U
#define INVALID_PGNO ((uint32_t)0xFFFFFFFFU)

#define PAGE_META     0
#define PAGE_INTERNAL 1
#define PAGE_LEAF     2

#define INT_HDR   16U
#define LEAF_HDR  20U
#define SLOT_SZ   2U
#define ICELL_HDR 6U
#define LCELL_HDR 4U
#define MAX_DEPTH 32

#define META_MAGIC   0
#define META_VERSION 4
#define META_TXNID   8
#define META_ROOT    16
#define META_FREE    20
#define META_NPAGES  24
#define META_CKSUM   28

/* ================================================================== */
/* Unaligned helpers                                                    */
/* ================================================================== */

static inline uint16_t rd16(const void *p){ uint16_t v; memcpy(&v,p,2); return v; }
static inline uint32_t rd32(const void *p){ uint32_t v; memcpy(&v,p,4); return v; }
static inline uint64_t rd64(const void *p){ uint64_t v; memcpy(&v,p,8); return v; }
static inline void wr16(void *p,uint16_t v){ memcpy(p,&v,2); }
static inline void wr32(void *p,uint32_t v){ memcpy(p,&v,4); }
static inline void wr64(void *p,uint64_t v){ memcpy(p,&v,8); }

/* ================================================================== */
/* Page field macros                                                    */
/* ================================================================== */

#define PB(pg,off) ((uint8_t*)(pg)+(uint32_t)(off))

#define PG_TYPE(pg)         (*(uint8_t*)PB(pg,0))
#define PG_NUM(pg)          rd16(PB(pg,2))
#define PG_PGNO(pg)         rd32(PB(pg,4))
#define SET_PG_TYPE(pg,v)   (*(uint8_t*)PB(pg,0)=(uint8_t)(v))
#define SET_PG_NUM(pg,v)    wr16(PB(pg,2),(uint16_t)(v))
#define SET_PG_PGNO(pg,v)   wr32(PB(pg,4),(v))

#define I_LEFT(pg)          rd32(PB(pg,8))
#define I_DEND(pg)          rd16(PB(pg,12))
#define SET_I_LEFT(pg,v)    wr32(PB(pg,8),(v))
#define SET_I_DEND(pg,v)    wr16(PB(pg,12),(uint16_t)(v))
#define I_SLOT(pg,i)        rd16(PB(pg,INT_HDR+(uint32_t)(i)*SLOT_SZ))
#define SET_I_SLOT(pg,i,v)  wr16(PB(pg,INT_HDR+(uint32_t)(i)*SLOT_SZ),(uint16_t)(v))
#define I_CKLEN(pg,off)     rd16(PB(pg,off))
#define I_CRIGHT(pg,off)    rd32(PB(pg,(uint32_t)(off)+2))
#define I_CKEY(pg,off)      PB(pg,(uint32_t)(off)+ICELL_HDR)
#define SET_I_CRIGHT(pg,off,v) wr32(PB(pg,(uint32_t)(off)+2),(v))
#define ICELL_SZ(klen)      (ICELL_HDR+(uint32_t)(klen))
#define I_FREE(pg)          ((uint32_t)(I_DEND(pg))-INT_HDR-(uint32_t)PG_NUM(pg)*SLOT_SZ)

#define L_RSIB(pg)          rd32(PB(pg,8))
#define L_LSIB(pg)          rd32(PB(pg,12))
#define L_DEND(pg)          rd16(PB(pg,16))
#define SET_L_RSIB(pg,v)    wr32(PB(pg,8),(v))
#define SET_L_LSIB(pg,v)    wr32(PB(pg,12),(v))
#define SET_L_DEND(pg,v)    wr16(PB(pg,16),(uint16_t)(v))
#define L_SLOT(pg,i)        rd16(PB(pg,LEAF_HDR+(uint32_t)(i)*SLOT_SZ))
#define SET_L_SLOT(pg,i,v)  wr16(PB(pg,LEAF_HDR+(uint32_t)(i)*SLOT_SZ),(uint16_t)(v))
#define L_CKLEN(pg,off)     rd16(PB(pg,off))
#define L_CVLEN(pg,off)     rd16(PB(pg,(uint32_t)(off)+2))
#define L_CKEY(pg,off)      PB(pg,(uint32_t)(off)+LCELL_HDR)
#define L_CVAL(pg,off,kl)   PB(pg,(uint32_t)(off)+LCELL_HDR+(uint32_t)(kl))
#define LCELL_SZ(kl,vl)     (LCELL_HDR+(uint32_t)(kl)+(uint32_t)(vl))
#define L_FREE(pg)          ((uint32_t)(L_DEND(pg))-LEAF_HDR-(uint32_t)PG_NUM(pg)*SLOT_SZ)

/* ================================================================== */
/* Structures                                                           */
/* ================================================================== */

struct DB {
    PageAllocator *alloc;
    uint32_t       page_size;
    void         **pages;
    uint32_t       pages_cap;
    uint64_t       txnid;
    uint32_t       root_pgno;
    uint32_t       free_pgno;
    uint32_t       num_pages;
    struct Txn    *write_txn;

    /* Active read-transaction snapshot txnids (for deferred GC). */
    uint64_t      *active_readers;
    uint32_t       num_readers;
    uint32_t       cap_readers;

    /* Pages freed by committed write txns that may still be readable.
     * Each entry: {freed_at txnid, pgno}.  A page is safe to reuse only
     * when every active reader started after freed_at. */
    struct { uint64_t freed_at; uint32_t pgno; } *deferred;
    uint32_t  num_deferred;
    uint32_t  cap_deferred;
};

struct Txn {
    struct DB    *db;
    struct Txn   *parent;
    uint64_t      txnid;
    unsigned int  flags;
    uint32_t      root_pgno;
    uint32_t      free_pgno;
    uint32_t      num_pages;
    uint32_t      saved_root;
    uint32_t      saved_free;
    uint32_t      saved_npages;
    uint32_t     *new_pages; uint32_t new_cnt; uint32_t new_cap;
    uint32_t     *old_pages; uint32_t old_cnt; uint32_t old_cap;
};

struct Cursor {
    struct Txn *txn;
    uint32_t    stack[MAX_DEPTH];
    int         idx  [MAX_DEPTH];
    int         depth;
};

/* ================================================================== */
/* Sorted uint32 array helpers                                          */
/* ================================================================== */

static int u32_find(const uint32_t *a, uint32_t n, uint32_t v, uint32_t *pos)
{
    uint32_t lo=0, hi=n;
    while (lo<hi) {
        uint32_t mid=lo+(hi-lo)/2;
        if (a[mid]==v){ if(pos)*pos=mid; return 1; }
        if (a[mid]<v) lo=mid+1; else hi=mid;
    }
    if (pos) *pos=lo;
    return 0;
}

static int u32_push(uint32_t **a, uint32_t *cnt, uint32_t *cap, uint32_t v)
{
    uint32_t pos;
    if (u32_find(*a,*cnt,v,&pos)) return 0;
    if (*cnt>=*cap) {
        uint32_t nc=*cap?*cap*2:16;
        uint32_t *na=(uint32_t*)realloc(*a,nc*sizeof(uint32_t));
        if (!na) return -1;
        *a=na; *cap=nc;
    }
    if (*cnt>pos)
        memmove(*a+pos+1, *a+pos, (*cnt-pos)*sizeof(uint32_t));
    (*a)[pos]=v; (*cnt)++;
    return 0;
}

static int u32_remove(uint32_t *a, uint32_t *cnt, uint32_t v)
{
    uint32_t pos;
    if (!u32_find(a,*cnt,v,&pos)) return 0;
    if (*cnt-pos-1>0)
        memmove(a+pos, a+pos+1, (*cnt-pos-1)*sizeof(uint32_t));
    (*cnt)--;
    return 1;
}

/* ================================================================== */
/* Key comparison                                                       */
/* ================================================================== */

static int keycmp(const void *a, uint32_t al, const void *b, uint32_t bl)
{
    uint32_t m=al<bl?al:bl;
    int c=memcmp(a,b,(size_t)m);
    if (c) return c;
    return al<bl?-1:al>bl?1:0;
}

/* ================================================================== */
/* Meta-page checksum                                                   */
/* ================================================================== */

static uint32_t meta_cksum(const void *pg)
{
    uint32_t s=rd32(PB(pg,META_MAGIC))^rd32(PB(pg,META_VERSION));
    uint64_t t=rd64(PB(pg,META_TXNID));
    s^=(uint32_t)t^(uint32_t)(t>>32);
    s^=rd32(PB(pg,META_ROOT))^rd32(PB(pg,META_FREE))^rd32(PB(pg,META_NPAGES));
    return s^0xDEADBEEFU;
}

/* ================================================================== */
/* Page initialisation                                                  */
/* ================================================================== */

static void pg_init_internal(void *pg, uint32_t pgno, uint32_t pgsz)
{
    memset(pg,0,pgsz);
    SET_PG_TYPE(pg,PAGE_INTERNAL); SET_PG_PGNO(pg,pgno); SET_PG_NUM(pg,0);
    SET_I_LEFT(pg,INVALID_PGNO);   SET_I_DEND(pg,(uint16_t)pgsz);
}

static void pg_init_leaf(void *pg, uint32_t pgno, uint32_t pgsz)
{
    memset(pg,0,pgsz);
    SET_PG_TYPE(pg,PAGE_LEAF); SET_PG_PGNO(pg,pgno); SET_PG_NUM(pg,0);
    SET_L_RSIB(pg,INVALID_PGNO); SET_L_LSIB(pg,INVALID_PGNO);
    SET_L_DEND(pg,(uint16_t)pgsz);
}

/* ================================================================== */
/* Raw page allocation (no tracking)                                    */
/* ================================================================== */

static uint32_t raw_alloc(struct Txn *txn)
{
    struct DB *db=txn->db;
    if (txn->free_pgno!=INVALID_PGNO) {
        uint32_t pgno=txn->free_pgno;
        txn->free_pgno=rd32(db->pages[pgno]);
        memset(db->pages[pgno],0,db->page_size);
        return pgno;
    }
    uint32_t pgno=txn->num_pages;
    if (pgno>=db->pages_cap) {
        uint32_t nc=db->pages_cap?db->pages_cap*2:64;
        void **np=(void**)realloc(db->pages,nc*sizeof(void*));
        if (!np) return INVALID_PGNO;
        memset(np+db->pages_cap,0,(nc-db->pages_cap)*sizeof(void*));
        db->pages=np; db->pages_cap=nc;
    }
    void *pg=db->alloc->alloc_page(db->alloc->ctx,db->page_size);
    if (!pg) return INVALID_PGNO;
    memset(pg,0,db->page_size);
    db->pages[pgno]=pg;
    txn->num_pages++;
    return pgno;
}

static uint32_t txn_alloc(struct Txn *txn)
{
    uint32_t pgno=raw_alloc(txn);
    if (pgno==INVALID_PGNO) return INVALID_PGNO;
    if (u32_push(&txn->new_pages,&txn->new_cnt,&txn->new_cap,pgno)<0)
        return INVALID_PGNO;
    return pgno;
}

/* ================================================================== */
/* Copy-on-write                                                        */
/* ================================================================== */

static uint32_t txn_cow(struct Txn *txn, uint32_t pgno)
{
    if (pgno==INVALID_PGNO) return INVALID_PGNO;
    if (u32_find(txn->new_pages,txn->new_cnt,pgno,NULL)) return pgno;
    struct DB *db=txn->db;
    uint32_t np=raw_alloc(txn);
    if (np==INVALID_PGNO) return INVALID_PGNO;
    memcpy(db->pages[np],db->pages[pgno],db->page_size);
    SET_PG_PGNO(db->pages[np],np);
    if (u32_push(&txn->new_pages,&txn->new_cnt,&txn->new_cap,np)<0) return INVALID_PGNO;
    if (u32_push(&txn->old_pages,&txn->old_cnt,&txn->old_cap,pgno)<0) return INVALID_PGNO;
    return np;
}

/* ================================================================== */
/* Deferred free-list management (MVCC GC)                              */
/* ================================================================== */

/* Move deferred pages whose freed_at is older than every active reader
 * onto the real free list.  Called whenever a reader ends. */
static void db_process_deferred(struct DB *db)
{
    uint64_t min_reader = UINT64_MAX;
    for (uint32_t i = 0; i < db->num_readers; i++)
        if (db->active_readers[i] < min_reader)
            min_reader = db->active_readers[i];

    uint32_t keep = 0;
    for (uint32_t i = 0; i < db->num_deferred; i++) {
        if (db->num_readers == 0 || db->deferred[i].freed_at < min_reader) {
            /* Safe to recycle */
            uint32_t p = db->deferred[i].pgno;
            wr32(db->pages[p], db->free_pgno);
            db->free_pgno = p;
        } else {
            db->deferred[keep++] = db->deferred[i];
        }
    }
    db->num_deferred = keep;
}

/* Add a page freed by a committed write txn to the deferred list. */
static int db_defer_page(struct DB *db, uint64_t freed_at, uint32_t pgno)
{
    if (db->num_deferred >= db->cap_deferred) {
        uint32_t nc = db->cap_deferred ? db->cap_deferred * 2 : 16;
        void *nd = realloc(db->deferred, nc * sizeof(db->deferred[0]));
        if (!nd) return -1;
        db->deferred = nd;
        db->cap_deferred = nc;
    }
    db->deferred[db->num_deferred].freed_at = freed_at;
    db->deferred[db->num_deferred].pgno     = pgno;
    db->num_deferred++;
    return 0;
}

/* Register/deregister an active read-transaction snapshot. */
static void db_add_reader(struct DB *db, uint64_t snap_txnid)
{
    if (db->num_readers >= db->cap_readers) {
        uint32_t nc = db->cap_readers ? db->cap_readers * 2 : 8;
        uint64_t *na = (uint64_t *)realloc(db->active_readers,
                                            nc * sizeof(uint64_t));
        if (!na) return;
        db->active_readers = na;
        db->cap_readers    = nc;
    }
    db->active_readers[db->num_readers++] = snap_txnid;
}

static void db_remove_reader(struct DB *db, uint64_t snap_txnid)
{
    for (uint32_t i = 0; i < db->num_readers; i++) {
        if (db->active_readers[i] == snap_txnid) {
            db->active_readers[i] =
                db->active_readers[--db->num_readers];
            break;
        }
    }
    db_process_deferred(db);
}

/* ================================================================== */
/* Meta-page management                                                 */
/* ================================================================== */

static void meta_write(struct DB *db)
{
    void *m0=db->pages[0], *m1=db->pages[1];
    uint64_t t0=rd64(PB(m0,META_TXNID)), t1=rd64(PB(m1,META_TXNID));
    void *dst=(t1>t0)?m0:m1;
    memset(dst,0,db->page_size);
    wr32(PB(dst,META_MAGIC),SAP_MAGIC); wr32(PB(dst,META_VERSION),SAP_VERSION);
    wr64(PB(dst,META_TXNID),db->txnid); wr32(PB(dst,META_ROOT),db->root_pgno);
    wr32(PB(dst,META_FREE),db->free_pgno); wr32(PB(dst,META_NPAGES),db->num_pages);
    wr32(PB(dst,META_CKSUM),meta_cksum(dst));
}

static int meta_load(struct DB *db)
{
    void *m0=db->pages[0], *m1=db->pages[1];
    int ok0=(rd32(PB(m0,META_MAGIC))==SAP_MAGIC)&&(rd32(PB(m0,META_CKSUM))==meta_cksum(m0));
    int ok1=(rd32(PB(m1,META_MAGIC))==SAP_MAGIC)&&(rd32(PB(m1,META_CKSUM))==meta_cksum(m1));
    void *best=NULL;
    if (ok0&&ok1) best=(rd64(PB(m0,META_TXNID))>=rd64(PB(m1,META_TXNID)))?m0:m1;
    else if (ok0) best=m0; else if (ok1) best=m1; else return -1;
    db->txnid=rd64(PB(best,META_TXNID)); db->root_pgno=rd32(PB(best,META_ROOT));
    db->free_pgno=rd32(PB(best,META_FREE)); db->num_pages=rd32(PB(best,META_NPAGES));
    return 0;
}

/* ================================================================== */
/* Leaf operations                                                      */
/* ================================================================== */

static int leaf_find(const void *pg, const void *key, uint32_t klen, int *found)
{
    int n=(int)PG_NUM(pg), lo=0, hi=n-1, pos=n;
    *found=0;
    while (lo<=hi) {
        int mid=lo+(hi-lo)/2;
        uint16_t off=(uint16_t)L_SLOT(pg,mid);
        int cmp=keycmp(L_CKEY(pg,off),L_CKLEN(pg,off),key,klen);
        if (cmp==0){ *found=1; return mid; }
        if (cmp>0){ pos=mid; hi=mid-1; } else lo=mid+1;
    }
    return pos;
}

static int leaf_insert(void *pg, int pos,
                       const void *key, uint16_t klen,
                       const void *val, uint16_t vlen)
{
    uint32_t need=SLOT_SZ+LCELL_SZ(klen,vlen);
    if (need>L_FREE(pg)) return -1;
    uint16_t dend=(uint16_t)L_DEND(pg);
    uint16_t coff=(uint16_t)(dend-LCELL_SZ(klen,vlen));
    wr16(PB(pg,coff),klen); wr16(PB(pg,coff+2),vlen);
    memcpy(PB(pg,coff+LCELL_HDR),key,klen);
    memcpy(PB(pg,coff+LCELL_HDR+klen),val,vlen);
    SET_L_DEND(pg,coff);
    int n=(int)PG_NUM(pg);
    if (n>pos)
        memmove(PB(pg,LEAF_HDR+(uint32_t)(pos+1)*SLOT_SZ),
                PB(pg,LEAF_HDR+(uint32_t)pos*SLOT_SZ),
                (uint32_t)(n-pos)*SLOT_SZ);
    SET_L_SLOT(pg,pos,coff); SET_PG_NUM(pg,(uint16_t)(n+1));
    return 0;
}

static void leaf_remove(void *pg, int pos)
{
    int n=(int)PG_NUM(pg);
    uint16_t off=(uint16_t)L_SLOT(pg,pos);
    uint32_t csz=LCELL_SZ(L_CKLEN(pg,off),L_CVLEN(pg,off));
    uint16_t dend=(uint16_t)L_DEND(pg);
    if (off>dend)
        memmove(PB(pg,dend+csz),PB(pg,dend),(uint32_t)(off-dend));
    for (int i=0;i<n;i++){
        if (i==pos) continue;
        uint16_t s=(uint16_t)L_SLOT(pg,i);
        if (s>=dend&&s<off) SET_L_SLOT(pg,i,(uint16_t)(s+csz));
    }
    SET_L_DEND(pg,(uint16_t)(dend+csz));
    memmove(PB(pg,LEAF_HDR+(uint32_t)pos*SLOT_SZ),
            PB(pg,LEAF_HDR+(uint32_t)(pos+1)*SLOT_SZ),
            (uint32_t)(n-pos-1)*SLOT_SZ);
    SET_PG_NUM(pg,(uint16_t)(n-1));
}

/* ================================================================== */
/* Internal node operations                                             */
/* ================================================================== */

static int int_find_child(const void *pg, const void *key, uint32_t klen)
{
    int n=(int)PG_NUM(pg), lo=0, hi=n-1, idx=n;
    while (lo<=hi) {
        int mid=lo+(hi-lo)/2;
        uint16_t off=(uint16_t)I_SLOT(pg,mid);
        int cmp=keycmp(I_CKEY(pg,off),I_CKLEN(pg,off),key,klen);
        if (cmp>0){ idx=mid; hi=mid-1; } else lo=mid+1;
    }
    return idx;
}

static uint32_t int_child(const void *pg, int idx)
{
    if (idx==0) return I_LEFT(pg);
    return I_CRIGHT(pg,I_SLOT(pg,idx-1));
}

static int int_insert(void *pg, int pos,
                      const void *key, uint16_t klen, uint32_t right_child)
{
    uint32_t need=SLOT_SZ+ICELL_SZ(klen);
    if (need>I_FREE(pg)) return -1;
    uint16_t dend=(uint16_t)I_DEND(pg);
    uint16_t coff=(uint16_t)(dend-ICELL_SZ(klen));
    wr16(PB(pg,coff),klen); wr32(PB(pg,coff+2),right_child);
    memcpy(PB(pg,coff+ICELL_HDR),key,klen);
    SET_I_DEND(pg,coff);
    int n=(int)PG_NUM(pg);
    if (n>pos)
        memmove(PB(pg,INT_HDR+(uint32_t)(pos+1)*SLOT_SZ),
                PB(pg,INT_HDR+(uint32_t)pos*SLOT_SZ),
                (uint32_t)(n-pos)*SLOT_SZ);
    SET_I_SLOT(pg,pos,coff); SET_PG_NUM(pg,(uint16_t)(n+1));
    return 0;
}

static void int_remove_child(void *pg, int child_idx)
{
    int slot_idx=(child_idx==0)?0:child_idx-1;
    int n=(int)PG_NUM(pg);
    if (child_idx==0) {
        uint16_t off=(uint16_t)I_SLOT(pg,0);
        SET_I_LEFT(pg,I_CRIGHT(pg,off));
    }
    uint16_t off=(uint16_t)I_SLOT(pg,slot_idx);
    uint32_t csz=ICELL_SZ(I_CKLEN(pg,off));
    uint16_t dend=(uint16_t)I_DEND(pg);
    if (off>dend)
        memmove(PB(pg,dend+csz),PB(pg,dend),(uint32_t)(off-dend));
    for (int i=0;i<n;i++){
        if (i==slot_idx) continue;
        uint16_t s=(uint16_t)I_SLOT(pg,i);
        if (s>=dend&&s<off) SET_I_SLOT(pg,i,(uint16_t)(s+csz));
    }
    SET_I_DEND(pg,(uint16_t)(dend+csz));
    memmove(PB(pg,INT_HDR+(uint32_t)slot_idx*SLOT_SZ),
            PB(pg,INT_HDR+(uint32_t)(slot_idx+1)*SLOT_SZ),
            (uint32_t)(n-slot_idx-1)*SLOT_SZ);
    SET_PG_NUM(pg,(uint16_t)(n-1));
}

/* ================================================================== */
/* Leaf split                                                           */
/* ================================================================== */

static uint32_t leaf_split(struct Txn *txn,
                            uint32_t lpgno, void *lpg,
                            const void *key, uint16_t klen,
                            const void *val, uint16_t vlen,
                            uint8_t *sep_buf,
                            uint16_t *sep_klen_out)
{
    struct DB *db=txn->db;
    int n=(int)PG_NUM(lpg);
    int total=n+1;

    /* Collect all kv pairs (existing + new) into temporary arrays */
    typedef struct { const uint8_t *k; uint16_t kl; const uint8_t *v; uint16_t vl; } KV;
    KV *all=(KV*)malloc((uint32_t)total*sizeof(KV));
    if (!all) return INVALID_PGNO;

    int found, ins=leaf_find(lpg,key,klen,&found);
    for (int j=0,i=0; j<total; j++) {
        if (j==ins){
            all[j].k=(const uint8_t*)key; all[j].kl=klen;
            all[j].v=(const uint8_t*)val; all[j].vl=vlen;
        } else {
            uint16_t off=(uint16_t)L_SLOT(lpg,i);
            all[j].k=L_CKEY(lpg,off); all[j].kl=L_CKLEN(lpg,off);
            all[j].v=L_CVAL(lpg,off,all[j].kl); all[j].vl=L_CVLEN(lpg,off);
            i++;
        }
    }

    /* We need to copy the kv data because we're about to reinit lpg */
    uint8_t *kbuf=(uint8_t*)malloc((uint32_t)total*db->page_size);
    uint8_t *vbuf=(uint8_t*)malloc((uint32_t)total*db->page_size);
    uint32_t *koff=(uint32_t*)malloc((uint32_t)total*sizeof(uint32_t));
    uint32_t *voff=(uint32_t*)malloc((uint32_t)total*sizeof(uint32_t));
    uint32_t *kl2 =(uint32_t*)malloc((uint32_t)total*sizeof(uint32_t));
    uint32_t *vl2 =(uint32_t*)malloc((uint32_t)total*sizeof(uint32_t));
    if (!kbuf||!vbuf||!koff||!voff||!kl2||!vl2){
        free(all);free(kbuf);free(vbuf);free(koff);free(voff);free(kl2);free(vl2);
        return INVALID_PGNO;
    }
    uint32_t ko=0,vo=0;
    for (int j=0;j<total;j++){
        kl2[j]=all[j].kl; vl2[j]=all[j].vl;
        koff[j]=ko; voff[j]=vo;
        memcpy(kbuf+ko,all[j].k,all[j].kl); ko+=all[j].kl;
        memcpy(vbuf+vo,all[j].v,all[j].vl); vo+=all[j].vl;
    }
    free(all);

    int left_n=total/2;

    /* Allocate right leaf */
    uint32_t rpgno=txn_alloc(txn);
    if (rpgno==INVALID_PGNO){
        free(kbuf);free(vbuf);free(koff);free(voff);free(kl2);free(vl2);
        return INVALID_PGNO;
    }
    void *rpg=db->pages[rpgno];

    /* Save sibling info before reinitialising lpg */
    uint32_t old_rsib=L_RSIB(lpg);

    pg_init_leaf(rpg,rpgno,db->page_size);
    pg_init_leaf(lpg,lpgno,db->page_size);

    /* Fix sibling chain for the two new halves (no COW of old_rsib needed;
     * L_LSIB of old_rsib is unused by the stack-based cursor). */
    SET_L_RSIB(lpg,rpgno);
    SET_L_RSIB(rpg,old_rsib);

    for (int j=0;j<total;j++){
        void *dst=(j<left_n)?lpg:rpg;
        int   dpos=(j<left_n)?j:j-left_n;
        leaf_insert(dst,dpos,kbuf+koff[j],(uint16_t)kl2[j],
                             vbuf+voff[j],(uint16_t)vl2[j]);
    }

    /* Separator = first key of right page */
    uint16_t sep_off=(uint16_t)L_SLOT(rpg,0);
    uint16_t sk=L_CKLEN(rpg,sep_off);
    memcpy(sep_buf,L_CKEY(rpg,sep_off),sk);
    *sep_klen_out=sk;

    free(kbuf);free(vbuf);free(koff);free(voff);free(kl2);free(vl2);
    return rpgno;
}

/* ================================================================== */
/* Internal node split                                                  */
/* ================================================================== */

static uint32_t int_split(struct Txn *txn,
                           uint32_t lpgno, void *lpg,
                           int ins_pos,
                           const void *key, uint16_t klen, uint32_t right_child,
                           uint8_t *sep_buf, uint16_t *sep_klen_out)
{
    struct DB *db=txn->db;
    int n=(int)PG_NUM(lpg);
    int total=n+1;

    uint16_t *ckl=(uint16_t*)malloc((uint32_t)total*sizeof(uint16_t));
    uint32_t *crc=(uint32_t*)malloc((uint32_t)total*sizeof(uint32_t));
    uint8_t  *kb =(uint8_t *)malloc((uint32_t)total*db->page_size);
    uint32_t *ko =(uint32_t*)malloc((uint32_t)total*sizeof(uint32_t));
    if (!ckl||!crc||!kb||!ko){
        free(ckl);free(crc);free(kb);free(ko); return INVALID_PGNO;
    }

    uint32_t kboff=0;
    for (int j=0,i=0; j<total; j++){
        if (j==ins_pos){
            ckl[j]=klen; crc[j]=right_child;
            memcpy(kb+kboff,key,klen); ko[j]=kboff; kboff+=klen;
        } else {
            uint16_t off=(uint16_t)I_SLOT(lpg,i);
            ckl[j]=I_CKLEN(lpg,off); crc[j]=I_CRIGHT(lpg,off);
            memcpy(kb+kboff,I_CKEY(lpg,off),ckl[j]); ko[j]=kboff; kboff+=ckl[j];
            i++;
        }
    }

    int mid=total/2;

    /* Separator (pushed-up key) */
    *sep_klen_out=ckl[mid];
    memcpy(sep_buf,kb+ko[mid],ckl[mid]);

    /* Right node left_child = child at position mid+1 in the merged sequence
     * Merged children: [I_LEFT(lpg), crc[0], crc[1], ..., crc[n]]
     * child at position k = (k==0)?I_LEFT(lpg):crc[k-1] (before insert)
     * After conceptual insert at ins_pos:
     *   merged_children[k] = (k==0)         ? I_LEFT(lpg)
     *                       :(k-1 < ins_pos) ? I_CRIGHT(lpg,I_SLOT(lpg,k-1))
     *                       :(k-1==ins_pos)  ? right_child
     *                       :                  I_CRIGHT(lpg,I_SLOT(lpg,k-2))
     * We need child at position mid+1. */
    uint32_t right_lc;
    int rpos=mid+1;
    if (rpos==0) right_lc=I_LEFT(lpg);
    else {
        int ri=rpos-1; /* index into merged crc */
        if      (ri < ins_pos) right_lc=I_CRIGHT(lpg,I_SLOT(lpg,ri));
        else if (ri==ins_pos)  right_lc=right_child;
        else                   right_lc=I_CRIGHT(lpg,I_SLOT(lpg,ri-1));
    }

    uint32_t rpgno=txn_alloc(txn);
    if (rpgno==INVALID_PGNO){
        free(ckl);free(crc);free(kb);free(ko); return INVALID_PGNO;
    }
    void *rpg=db->pages[rpgno];
    uint32_t old_left=I_LEFT(lpg);
    pg_init_internal(rpg,rpgno,db->page_size);
    pg_init_internal(lpg,lpgno,db->page_size);
    SET_I_LEFT(lpg,old_left);
    SET_I_LEFT(rpg,right_lc);

    for (int j=0; j<total; j++){
        if (j==mid) continue; /* pushed up */
        if (j<mid)  int_insert(lpg,j,       kb+ko[j],ckl[j],crc[j]);
        else        int_insert(rpg,j-mid-1,  kb+ko[j],ckl[j],crc[j]);
    }

    free(ckl);free(crc);free(kb);free(ko);
    return rpgno;
}

/* ================================================================== */
/* txn_put                                                              */
/* ================================================================== */

int txn_put(Txn *txn_pub,
            const void *key, uint32_t key_len,
            const void *val, uint32_t val_len)
{
    struct Txn *txn=(struct Txn*)txn_pub;
    if (txn->flags&TXN_RDONLY) return SAP_READONLY;
    struct DB *db=txn->db;

    if (SLOT_SZ+LCELL_SZ(key_len,val_len)+LEAF_HDR>db->page_size) return SAP_FULL;

    if (txn->root_pgno==INVALID_PGNO) {
        uint32_t pgno=txn_alloc(txn);
        if (pgno==INVALID_PGNO) return SAP_ERROR;
        pg_init_leaf(db->pages[pgno],pgno,db->page_size);
        if (leaf_insert(db->pages[pgno],0,key,(uint16_t)key_len,val,(uint16_t)val_len)<0)
            return SAP_ERROR;
        txn->root_pgno=pgno;
        return SAP_OK;
    }

    /* Collect path */
    uint32_t path[MAX_DEPTH]; int path_idx[MAX_DEPTH]; int depth=0;
    uint32_t pgno=txn->root_pgno;
    while (PG_TYPE(db->pages[pgno])==PAGE_INTERNAL) {
        void *pg=db->pages[pgno];
        int idx=int_find_child(pg,key,key_len);
        if (depth>=MAX_DEPTH-1) return SAP_ERROR;
        path[depth]=pgno; path_idx[depth]=idx; depth++;
        pgno=int_child(pg,idx);
    }

    /* COW leaf */
    uint32_t leaf_pgno=txn_cow(txn,pgno);
    if (leaf_pgno==INVALID_PGNO) return SAP_ERROR;

    /* COW ancestors and update child references bottom-up */
    if (depth>0) {
        uint32_t child=leaf_pgno;
        for (int d=depth-1; d>=0; d--) {
            uint32_t pp=txn_cow(txn,path[d]);
            if (pp==INVALID_PGNO) return SAP_ERROR;
            path[d]=pp;
            void *par=db->pages[pp];
            int ci=path_idx[d];
            if (ci==0) SET_I_LEFT(par,child);
            else SET_I_CRIGHT(par,I_SLOT(par,ci-1),child);
            child=pp;
        }
        txn->root_pgno=path[0];
    } else {
        txn->root_pgno=leaf_pgno;
    }

    void *lpg=db->pages[leaf_pgno];

    /* Update existing key by remove+reinsert */
    int found; int pos=leaf_find(lpg,key,key_len,&found);
    if (found) { leaf_remove(lpg,pos); pos=leaf_find(lpg,key,key_len,&found); }

    if (leaf_insert(lpg,pos,key,(uint16_t)key_len,val,(uint16_t)val_len)==0)
        return SAP_OK;

    /* Leaf full: split */
    uint8_t sep_buf[SAPLING_PAGE_SIZE/2+8];
    uint16_t sep_klen;
    uint32_t rpgno=leaf_split(txn,leaf_pgno,lpg,
                               key,(uint16_t)key_len,val,(uint16_t)val_len,
                               sep_buf,&sep_klen);
    if (rpgno==INVALID_PGNO) return SAP_ERROR;

    uint32_t left_pgno=leaf_pgno, right_pgno=rpgno;
    const void *sep_key=sep_buf;

    /* Propagate split up the path */
    for (int d=depth-1; d>=0; d--) {
        uint32_t par_pgno=path[d];
        void *par=db->pages[par_pgno];
        int ins_pos=path_idx[d];
        if (int_insert(par,ins_pos,sep_key,sep_klen,right_pgno)==0)
            return SAP_OK;
        /* Parent full: split internal node */
        uint8_t psep[SAPLING_PAGE_SIZE/2+8]; uint16_t psep_klen;
        uint32_t nr=int_split(txn,par_pgno,par,ins_pos,sep_key,sep_klen,right_pgno,
                               psep,&psep_klen);
        if (nr==INVALID_PGNO) return SAP_ERROR;
        memcpy(sep_buf,psep,psep_klen); sep_klen=psep_klen; sep_key=sep_buf;
        left_pgno=par_pgno; right_pgno=nr;
    }

    /* Root split: new root */
    uint32_t new_root=txn_alloc(txn);
    if (new_root==INVALID_PGNO) return SAP_ERROR;
    void *nr=db->pages[new_root];
    pg_init_internal(nr,new_root,db->page_size);
    SET_I_LEFT(nr,left_pgno);
    int_insert(nr,0,sep_key,sep_klen,right_pgno);
    txn->root_pgno=new_root;
    return SAP_OK;
}

/* ================================================================== */
/* txn_get                                                              */
/* ================================================================== */

int txn_get(Txn *txn_pub,
            const void *key, uint32_t key_len,
            const void **val_out, uint32_t *val_len_out)
{
    struct Txn *txn=(struct Txn*)txn_pub;
    struct DB  *db=txn->db;
    if (txn->root_pgno==INVALID_PGNO) return SAP_NOTFOUND;
    uint32_t pgno=txn->root_pgno;
    while (PG_TYPE(db->pages[pgno])==PAGE_INTERNAL){
        void *pg=db->pages[pgno];
        pgno=int_child(pg,int_find_child(pg,key,key_len));
    }
    void *lpg=db->pages[pgno];
    int found; int pos=leaf_find(lpg,key,key_len,&found);
    if (!found) return SAP_NOTFOUND;
    uint16_t off=(uint16_t)L_SLOT(lpg,pos);
    *val_out=L_CVAL(lpg,off,L_CKLEN(lpg,off));
    *val_len_out=L_CVLEN(lpg,off);
    return SAP_OK;
}

/* ================================================================== */
/* txn_del                                                              */
/* ================================================================== */

static void txn_free_page(struct Txn *txn, uint32_t pgno)
{
    struct DB *db=txn->db;
    wr32(db->pages[pgno],txn->free_pgno);
    txn->free_pgno=pgno;
    u32_remove(txn->new_pages,&txn->new_cnt,pgno);
}

int txn_del(Txn *txn_pub, const void *key, uint32_t key_len)
{
    struct Txn *txn=(struct Txn*)txn_pub;
    if (txn->flags&TXN_RDONLY) return SAP_READONLY;
    struct DB *db=txn->db;
    if (txn->root_pgno==INVALID_PGNO) return SAP_NOTFOUND;

    uint32_t path[MAX_DEPTH]; int path_idx[MAX_DEPTH]; int depth=0;
    uint32_t pgno=txn->root_pgno;
    while (PG_TYPE(db->pages[pgno])==PAGE_INTERNAL){
        void *pg=db->pages[pgno];
        int idx=int_find_child(pg,key,key_len);
        if (depth>=MAX_DEPTH-1) return SAP_ERROR;
        path[depth]=pgno; path_idx[depth]=idx; depth++;
        pgno=int_child(pg,idx);
    }

    /* COW leaf */
    uint32_t leaf_pgno=txn_cow(txn,pgno);
    if (leaf_pgno==INVALID_PGNO) return SAP_ERROR;
    void *lpg=db->pages[leaf_pgno];
    int found; int pos=leaf_find(lpg,key,key_len,&found);
    if (!found) return SAP_NOTFOUND;
    leaf_remove(lpg,pos);

    /* COW ancestors, update child refs */
    uint32_t child=leaf_pgno;
    for (int d=depth-1; d>=0; d--){
        uint32_t pp=txn_cow(txn,path[d]);
        if (pp==INVALID_PGNO) return SAP_ERROR;
        path[d]=pp;
        void *par=db->pages[pp];
        int ci=path_idx[d];
        if (ci==0) SET_I_LEFT(par,child); else SET_I_CRIGHT(par,I_SLOT(par,ci-1),child);
        child=pp;
    }
    if (depth>0) txn->root_pgno=path[0]; else txn->root_pgno=leaf_pgno;

    /* If leaf empty: unlink and collapse */
    if (PG_NUM(lpg)>0) return SAP_OK;

    /* Don't COW siblings: updating their parent references would require
     * knowing the sibling's parent, which is not on our path.  The
     * sibling L_RSIB/L_LSIB fields are only cosmetically stale; the
     * stack-based cursor does not follow sibling pointers. */
    txn_free_page(txn,leaf_pgno);

    if (depth==0){ txn->root_pgno=INVALID_PGNO; return SAP_OK; }

    /* Remove empty leaf from parent, propagate collapse */
    for (int d=depth-1; d>=0; d--){
        uint32_t par_pgno=path[d];
        void *par=db->pages[par_pgno];
        int_remove_child(par,path_idx[d]);
        if (PG_NUM(par)>0) break;
        /* Parent has 0 keys (1 child): collapse */
        uint32_t sole=I_LEFT(par);
        txn_free_page(txn,par_pgno);
        if (d==0){ txn->root_pgno=sole; break; }
        /* Replace parent reference in grandparent (key count unchanged: break) */
        uint32_t gp=path[d-1]; void *gpg=db->pages[gp];
        int gc=path_idx[d-1];
        if (gc==0) SET_I_LEFT(gpg,sole); else SET_I_CRIGHT(gpg,I_SLOT(gpg,gc-1),sole);
        break;  /* grandparent's key count did not change; stop propagating */
    }
    return SAP_OK;
}

/* ================================================================== */
/* Transaction management                                               */
/* ================================================================== */

static void txn_free_mem(struct Txn *t){ free(t->new_pages); free(t->old_pages); free(t); }

Txn *txn_begin(DB *db_pub, Txn *par_pub, unsigned int flags)
{
    struct DB  *db=(struct DB*)db_pub;
    struct Txn *par=(struct Txn*)par_pub;
    if (!(flags&TXN_RDONLY)&&!par&&db->write_txn) return NULL;
    struct Txn *txn=(struct Txn*)calloc(1,sizeof(*txn));
    if (!txn) return NULL;
    txn->db=db; txn->parent=par; txn->flags=flags;
    if (par){
        txn->txnid=par->txnid;
        txn->root_pgno=par->root_pgno; txn->free_pgno=par->free_pgno; txn->num_pages=par->num_pages;
        txn->saved_root=par->root_pgno; txn->saved_free=par->free_pgno; txn->saved_npages=par->num_pages;
    } else {
        txn->txnid=db->txnid;
        txn->root_pgno=db->root_pgno; txn->free_pgno=db->free_pgno; txn->num_pages=db->num_pages;
        txn->saved_root=db->root_pgno; txn->saved_free=db->free_pgno; txn->saved_npages=db->num_pages;
        if (!(flags&TXN_RDONLY)){
            db->write_txn=txn;
            db_process_deferred(db);           /* reclaim safe deferred pages */
            txn->free_pgno=db->free_pgno;      /* pick up newly recycled pages */
        } else {
            db_add_reader(db, txn->txnid);
        }
    }
    return (Txn*)txn;
}

int txn_commit(Txn *txn_pub)
{
    struct Txn *txn=(struct Txn*)txn_pub;
    struct DB  *db=txn->db;
    if (txn->flags&TXN_RDONLY){
        db_remove_reader(db, txn->txnid);
        txn_free_mem(txn); return SAP_OK;
    }
    if (txn->parent){
        struct Txn *par=txn->parent;
        par->root_pgno=txn->root_pgno; par->free_pgno=txn->free_pgno; par->num_pages=txn->num_pages;
        for (uint32_t i=0;i<txn->new_cnt;i++)
            u32_push(&par->new_pages,&par->new_cnt,&par->new_cap,txn->new_pages[i]);
        for (uint32_t i=0;i<txn->old_cnt;i++){
            uint32_t p=txn->old_pages[i];
            u32_remove(par->new_pages,&par->new_cnt,p);
            u32_push(&par->old_pages,&par->old_cnt,&par->old_cap,p);
        }
        txn_free_mem(txn); return SAP_OK;
    }
    uint64_t freed_at = txn->txnid;   /* snapshot txnid that could see these pages */
    txn->txnid++;
    /* Defer old_pages: they may still be readable by existing read txns */
    for (uint32_t i=0;i<txn->old_cnt;i++)
        db_defer_page(db, freed_at, txn->old_pages[i]);
    /* txn_free_page()-freed pages (empty nodes) are in txn->free_pgno chain
     * and are not in old_pages; they go straight to db->free_pgno since they
     * were COW'd by this txn and never visible to prior readers. */
    db->txnid=txn->txnid; db->root_pgno=txn->root_pgno;
    db->free_pgno=txn->free_pgno; db->num_pages=txn->num_pages;
    meta_write(db); db->write_txn=NULL;
    txn_free_mem(txn); return SAP_OK;
}

void txn_abort(Txn *txn_pub)
{
    struct Txn *txn=(struct Txn*)txn_pub;
    struct DB  *db=txn->db;
    if (txn->flags&TXN_RDONLY){
        db_remove_reader(db, txn->txnid);
        txn_free_mem(txn); return;
    }
    for (uint32_t i=0;i<txn->new_cnt;i++){
        uint32_t pgno=txn->new_pages[i];
        if (pgno>=db->num_pages){
            db->alloc->free_page(db->alloc->ctx,db->pages[pgno],db->page_size);
            db->pages[pgno]=NULL;
        } else {
            uint32_t *fh=txn->parent?&txn->parent->free_pgno:&db->free_pgno;
            wr32(db->pages[pgno],*fh); *fh=pgno;
        }
    }
    if (txn->parent){
        txn->parent->root_pgno=txn->saved_root;
        txn->parent->free_pgno=txn->saved_free;
        txn->parent->num_pages=txn->saved_npages;
    } else { db->write_txn=NULL; }
    txn_free_mem(txn);
}

/* ================================================================== */
/* Database lifecycle                                                   */
/* ================================================================== */

DB *db_open(PageAllocator *alloc, uint32_t page_size)
{
    if (page_size<256) return NULL;
    struct DB *db=(struct DB*)calloc(1,sizeof(*db));
    if (!db) return NULL;
    db->alloc=alloc; db->page_size=page_size;
    db->pages_cap=64;
    db->pages=(void**)calloc(db->pages_cap,sizeof(void*));
    if (!db->pages){ free(db); return NULL; }
    for (int i=0;i<2;i++){
        void *pg=alloc->alloc_page(alloc->ctx,page_size);
        if (!pg){ db_close((DB*)db); return NULL; }
        memset(pg,0,page_size); db->pages[i]=pg;
    }
    db->num_pages=2;
    if (meta_load(db)<0){
        db->txnid=0; db->root_pgno=INVALID_PGNO;
        db->free_pgno=INVALID_PGNO;
        meta_write(db);
        db->txnid=1; meta_write(db);
        db->txnid=0;
    }
    return (DB*)db;
}

uint32_t db_num_pages(DB *db_pub)
{
    struct DB *db=(struct DB*)db_pub;
    return db ? db->num_pages : 0;
}

void db_close(DB *db_pub)
{
    struct DB *db=(struct DB*)db_pub;
    if (!db) return;
    if (db->write_txn) txn_abort((Txn*)db->write_txn);
    if (db->pages){
        uint32_t lim=db->num_pages<db->pages_cap?db->num_pages:db->pages_cap;
        for (uint32_t i=0;i<lim;i++)
            if (db->pages[i]) db->alloc->free_page(db->alloc->ctx,db->pages[i],db->page_size);
        free(db->pages);
    }
    free(db->active_readers);
    free(db->deferred);
    free(db);
}

/* ================================================================== */
/* Cursor                                                               */
/* ================================================================== */

Cursor *cursor_open(Txn *txn_pub)
{
    struct Cursor *c=(struct Cursor*)calloc(1,sizeof(*c));
    if (!c) return NULL;
    c->txn=(struct Txn*)txn_pub; c->depth=-1;
    return (Cursor*)c;
}
void cursor_close(Cursor *c){ free(c); }

static void cursor_go_leftmost(struct Cursor *c, uint32_t pgno)
{
    struct DB *db=c->txn->db;
    while (PG_TYPE(db->pages[pgno])==PAGE_INTERNAL){
        void *pg=db->pages[pgno];
        c->stack[c->depth]=pgno; c->idx[c->depth]=0; c->depth++;
        pgno=I_LEFT(pg);
    }
    c->stack[c->depth]=pgno; c->idx[c->depth]=0;
}

static void cursor_go_rightmost(struct Cursor *c, uint32_t pgno)
{
    struct DB *db=c->txn->db;
    while (PG_TYPE(db->pages[pgno])==PAGE_INTERNAL){
        void *pg=db->pages[pgno];
        int n=(int)PG_NUM(pg);
        c->stack[c->depth]=pgno; c->idx[c->depth]=n; c->depth++;
        pgno=int_child(pg,n);
    }
    void *lpg=db->pages[pgno];
    int n=(int)PG_NUM(lpg);
    c->stack[c->depth]=pgno; c->idx[c->depth]=(n>0)?n-1:0;
}

int cursor_first(Cursor *cp)
{
    struct Cursor *c=(struct Cursor*)cp;
    struct Txn *txn=c->txn;
    if (txn->root_pgno==INVALID_PGNO){ c->depth=-1; return SAP_NOTFOUND; }
    c->depth=0;
    cursor_go_leftmost(c,txn->root_pgno);
    if (PG_NUM(txn->db->pages[c->stack[c->depth]])==0){ c->depth=-1; return SAP_NOTFOUND; }
    return SAP_OK;
}

int cursor_last(Cursor *cp)
{
    struct Cursor *c=(struct Cursor*)cp;
    struct Txn *txn=c->txn;
    if (txn->root_pgno==INVALID_PGNO){ c->depth=-1; return SAP_NOTFOUND; }
    c->depth=0;
    cursor_go_rightmost(c,txn->root_pgno);
    if (PG_NUM(txn->db->pages[c->stack[c->depth]])==0){ c->depth=-1; return SAP_NOTFOUND; }
    return SAP_OK;
}

int cursor_seek(Cursor *cp, const void *key, uint32_t key_len)
{
    struct Cursor *c=(struct Cursor*)cp;
    struct Txn *txn=c->txn; struct DB *db=txn->db;
    c->depth=-1;
    if (txn->root_pgno==INVALID_PGNO) return SAP_NOTFOUND;
    uint32_t pgno=txn->root_pgno;
    c->depth=0;
    while (PG_TYPE(db->pages[pgno])==PAGE_INTERNAL){
        void *pg=db->pages[pgno];
        int idx=int_find_child(pg,key,key_len);
        c->stack[c->depth]=pgno; c->idx[c->depth]=idx; c->depth++;
        pgno=int_child(pg,idx);
    }
    void *lpg=db->pages[pgno];
    int found; int pos=leaf_find(lpg,key,key_len,&found);
    c->stack[c->depth]=pgno; c->idx[c->depth]=pos;
    if (pos>=(int)PG_NUM(lpg)) return cursor_next(cp);
    return SAP_OK;
}

int cursor_next(Cursor *cp)
{
    struct Cursor *c=(struct Cursor*)cp;
    struct DB *db=c->txn->db;
    if (c->depth<0) return SAP_NOTFOUND;

    /* Advance within the current leaf. */
    void *lpg=db->pages[c->stack[c->depth]];
    c->idx[c->depth]++;
    if (c->idx[c->depth]<(int)PG_NUM(lpg)) return SAP_OK;

    /* At the end of the leaf: climb the ancestor stack until we find
     * an internal node where we can go right, then descend leftmost. */
    for (int d=c->depth-1; d>=0; d--) {
        void *pg=db->pages[c->stack[d]];
        if (c->idx[d]<(int)PG_NUM(pg)) {
            c->idx[d]++;
            uint32_t child=int_child(pg,c->idx[d]);
            c->depth=d+1;
            while (PG_TYPE(db->pages[child])==PAGE_INTERNAL) {
                void *cpg=db->pages[child];
                c->stack[c->depth]=child; c->idx[c->depth]=0; c->depth++;
                child=I_LEFT(cpg);
            }
            c->stack[c->depth]=child; c->idx[c->depth]=0;
            if (PG_NUM(db->pages[child])==0){ c->depth=-1; return SAP_NOTFOUND; }
            return SAP_OK;
        }
    }
    c->depth=-1;
    return SAP_NOTFOUND;
}

int cursor_prev(Cursor *cp)
{
    struct Cursor *c=(struct Cursor*)cp;
    struct DB *db=c->txn->db;
    if (c->depth<0) return SAP_NOTFOUND;

    /* Retreat within the current leaf. */
    if (c->idx[c->depth]>0){ c->idx[c->depth]--; return SAP_OK; }

    /* At the beginning of the leaf: climb until we can go left, then
     * descend to the rightmost leaf of that subtree. */
    for (int d=c->depth-1; d>=0; d--) {
        if (c->idx[d]>0) {
            c->idx[d]--;
            void *pg=db->pages[c->stack[d]];
            uint32_t child=int_child(pg,c->idx[d]);
            c->depth=d+1;
            while (PG_TYPE(db->pages[child])==PAGE_INTERNAL) {
                void *cpg=db->pages[child];
                int cn=(int)PG_NUM(cpg);
                c->stack[c->depth]=child; c->idx[c->depth]=cn; c->depth++;
                child=int_child(cpg,cn);
            }
            void *leaf=db->pages[child];
            int ln=(int)PG_NUM(leaf);
            c->stack[c->depth]=child; c->idx[c->depth]=(ln>0)?ln-1:0;
            if (ln==0){ c->depth=-1; return SAP_NOTFOUND; }
            return SAP_OK;
        }
    }
    c->depth=-1;
    return SAP_NOTFOUND;
}

int cursor_get(Cursor *cp,
               const void **key_out, uint32_t *key_len_out,
               const void **val_out,  uint32_t *val_len_out)
{
    struct Cursor *c=(struct Cursor*)cp;
    if (c->depth<0) return SAP_NOTFOUND;
    struct DB *db=c->txn->db;
    void *lpg=db->pages[c->stack[c->depth]];
    int pos=c->idx[c->depth];
    if (pos<0||pos>=(int)PG_NUM(lpg)) return SAP_NOTFOUND;
    uint16_t off=(uint16_t)L_SLOT(lpg,pos);
    uint16_t klen=L_CKLEN(lpg,off);
    *key_out=L_CKEY(lpg,off); *key_len_out=klen;
    *val_out=L_CVAL(lpg,off,klen); *val_len_out=L_CVLEN(lpg,off);
    return SAP_OK;
}
