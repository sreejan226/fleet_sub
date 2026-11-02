from __future__ import annotations

import hashlib
import logging
import re

_SECRET_RE = re.compile(r"(password|secret|credential|token|api[_-]?key)\s*[:=]\s*\S+", re.I)


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(str(record.msg))
        if record.args:
            record.args = tuple(redact(str(a)) for a in record.args)
        return True


def redact(text: str) -> str:
    return _SECRET_RE.sub("[REDACTED]", text)


def digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger().addFilter(RedactingFilter())
