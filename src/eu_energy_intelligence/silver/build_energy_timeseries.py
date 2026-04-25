from datetime import datetime, timedelta


def add_event_timestamps(
    rows: list[dict[str, object]],
    period_start: str,
) -> list[dict[str, object]]:
    start = datetime.strptime(period_start, "%Y%m%d%H%M")

    enriched = []

    for idx, row in enumerate(rows):
        position = row.get("position")

        if isinstance(position, int):
            offset_minutes = (position - 1) * 15
        else:
            offset_minutes = idx * 15

        event_ts = start + timedelta(minutes=offset_minutes)

        new_row = dict(row)
        new_row["event_timestamp_utc"] = event_ts.isoformat()
        new_row["event_date"] = event_ts.date().isoformat()

        enriched.append(new_row)

    return enriched
