from __future__ import annotations

import os
import sys


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "api"
    if cmd == "worker":
        from generation_fleet.api import worker_main

        worker_main()
        return
    if cmd == "loop":
        from generation_fleet.api import control_loop_main

        control_loop_main()
        return
    import uvicorn

    uvicorn.run("generation_fleet.api:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))


if __name__ == "__main__":
    main()
