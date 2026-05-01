#!/usr/bin/env python3
from __future__ import annotations

import uvicorn

from ib_qlib_pipeline.webapi.app import create_app
from ib_qlib_pipeline.webapi.settings import Settings


if __name__ == "__main__":
    settings = Settings.load()
    uvicorn.run(
        "ib_qlib_pipeline.webapi.app:create_app",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
