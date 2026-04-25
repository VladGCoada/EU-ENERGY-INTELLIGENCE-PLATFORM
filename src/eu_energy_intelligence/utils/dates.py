from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta


def date_range_days(start: datetime, end: datetime) -> Iterator[datetime]:
    """Yield each date between start and end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
