import os
import glob

tests = glob.glob('tests/unit/runner_*_test.c') + glob.glob('tests/unit/test_runner_*.c')

for test in tests:
    with open(test, 'r') as f:
        content = f.read()
    
    if 'sap_bept_subsystem_init' in content:
        continue
        
    if 'sap_runner_v0_bootstrap_dbis(db)' not in content:
        # Some tests might bootstrap manually or not at all, check for those
        print(f"Skipping {test}: no bootstrap call found")
        continue

    # Add include
    if '#include "sapling/bept.h"' not in content:
        content = content.replace('#include "runner/runner_v0.h"', '#include "runner/runner_v0.h"\n#include "sapling/bept.h"')

    # Find the bootstrap block
    target = """    if (sap_runner_v0_bootstrap_dbis(db) != SAP_OK)
    {
        db_close(db);
        return NULL;
    }"""
    
    replacement = target + """
    if (sap_bept_subsystem_init((SapEnv *)db) != SAP_OK)
    {
        db_close(db);
        return NULL;
    }"""
    
    if target in content:
        content = content.replace(target, replacement)
        with open(test, 'w') as f:
            f.write(content)
        print(f"Patched {test}")
    else:
        # Try a more relaxed search
        print(f"Failed to find exact bootstrap block in {test}")

