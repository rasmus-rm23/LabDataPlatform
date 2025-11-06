from datetime import datetime, timezone

def duration_xhxxmxxs(start: datetime, end: datetime) -> str:
    # Ensure both are timezone-aware UTC datetimes
    print(f'start time: {start}')
    print(f'end time: {end}')
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    delta = end - start
    total_seconds = int(delta.total_seconds())

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours}h{minutes:02d}m{seconds:02d}s"