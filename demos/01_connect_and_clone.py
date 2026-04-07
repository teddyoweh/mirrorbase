"""Connect to a database and create a clone."""

import sys
import mirrorbase

mb = mirrorbase.MirrorBase()
base_id = mb.connect(sys.argv[1])
clone_id, url = mb.clone(base_id)
print(f"Clone ready: {url}")

# cleanup
mb.teardown(base_id)
