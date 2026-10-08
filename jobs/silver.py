"""Workflow task: silver. Thin entry point; the logic is in lakehouse.tasks."""

import sys

# Bundle deploys sync src/ to the workspace; the job passes --src-root so the
# package imports without building a wheel.
for _arg in sys.argv[1:]:
    if _arg.startswith("--src-root=") and _arg.split("=", 1)[1]:
        sys.path.insert(0, _arg.split("=", 1)[1])

from lakehouse.cli import main  # noqa: E402

if __name__ == "__main__":
    main("silver")
