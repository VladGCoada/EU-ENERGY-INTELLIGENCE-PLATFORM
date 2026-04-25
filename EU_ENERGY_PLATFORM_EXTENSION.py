from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from eu_energy_intelligence.production_extension import *  # noqa: F401,F403


if __name__ == "__main__":
    main()
