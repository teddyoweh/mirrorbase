"""Benchmark clone speed."""

import sys
import time
import mirrorbase

mb = mirrorbase.MirrorBase()
base_id = mb.connect(sys.argv[1])
time.sleep(3)

for i in range(5):
    t = time.time()
    cid, _ = mb.clone(base_id, clone_id=f"bench-{i}")
    print(f"Clone {i}: {time.time() - t:.3f}s")

mb.teardown(base_id)
