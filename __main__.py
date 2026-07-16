from __future__ import annotations

import os

import uvicorn


def main() -> None:
    uvicorn.run(
        "orchestrator.app:app",
        host=os.environ.get("RUNNER_ORCHESTRATOR_HOST", "0.0.0.0"),
        port=int(os.environ.get("RUNNER_ORCHESTRATOR_PORT", "8080")),
        workers=1,
        proxy_headers=True,
    )


if __name__ == "__main__":
    main()
