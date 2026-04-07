"""Create multiple isolated clones from one source."""

import sys
import mirrorbase

mb = mirrorbase.MirrorBase()
base_id = mb.connect(sys.argv[1])

for name in ["dev", "staging", "ci"]:
    clone_id, url = mb.clone(base_id, clone_id=name)
    print(f"{name}: {url}")

print(f"\n{len(mb.list_clones())} clones running")

# cleanup
mb.teardown(base_id)
