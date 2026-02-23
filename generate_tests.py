def generate_c_struct(record):
    lines = [f"typedef struct __attribute__((packed)) {{"]
    for field in record.fields:
        c_type = "uint8_t" # fallback
        if field.wit_type == "s64": c_type = "int64_t"
        elif field.wit_type == "u64": c_type = "uint64_t"
        elif field.wit_type == "s32": c_type = "int32_t"
        elif field.wit_type == "u32": c_type = "uint32_t"
        elif field.wit_type == "score" or field.wit_type == "f64": c_type = "double"
        elif field.wit_type == "timestamp": c_type = "int64_t"
        elif field.wit_type == "bool": c_type = "uint8_t"
        
        # for bytes/utf8 strings, WIT component model passes pointer/length pairs or we can just model them as offsets.
        # But this is C payload validation. Let's make a generic SapWitString type for dynamic bytes.
        
        if field.wit_type in ("utf8", "bytes", "string"):
            lines.append(f"    uint32_t {field.c_name}_offset;")
            lines.append(f"    uint32_t {field.c_name}_len;")
        else:
            lines.append(f"    {c_type} {field.c_name};")
    lines.append(f"}} SapWit_{record.name.replace('-', '_')};")
    return "\n".join(lines)
