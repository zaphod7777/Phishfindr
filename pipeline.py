#!/usr/bin/env python3
"""
Top-level launcher so you can run: ./pipeline.py --output postgres --once

This makes sure the project root is on sys.path so `import phishfindr` works
even when invoking this script directly.
"""
from __future__ import annotations
import os
import sys

# Ensure project root (directory containing this file) is on sys.path first
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Now import package entrypoint (must exist at phishfindr/pipeline.py with main())
try:
    from phishfindr.pipeline import main
except Exception as e:
    # Helpful error if package can't be imported
    print("Failed to import phishfindr pipeline module:", e, file=sys.stderr)
    sys.exit(3)

if __name__ == "__main__":
    # Pass CLI args (exclude program name)
    sys.exit(main(sys.argv[1:]))
