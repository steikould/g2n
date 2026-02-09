"""Request audit middleware — logging and correlation ID injection."""

from __future__ import annotations

import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from g2n.common.logging import get_logger

logger = get_logger(__name__)

CORRELATION_ID_HEADER = "X-Correlation-ID"


class AuditMiddleware(BaseHTTPMiddleware):
    """Middleware that injects correlation IDs and logs request metadata."""

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        correlation_id = request.headers.get(
            CORRELATION_ID_HEADER, str(uuid.uuid4())
        )
        start = time.monotonic()

        logger.info(
            "request_start",
            method=request.method,
            path=request.url.path,
            correlation_id=correlation_id,
        )

        response = await call_next(request)

        duration_ms = (time.monotonic() - start) * 1000
        response.headers[CORRELATION_ID_HEADER] = correlation_id

        logger.info(
            "request_end",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration_ms, 2),
            correlation_id=correlation_id,
        )

        return response
