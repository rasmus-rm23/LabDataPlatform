from datetime import datetime, timezone
import pandas as pd

def duration_xhxxmxxs(start: datetime, end: datetime) -> str:
    # Ensure both are timezone-aware UTC datetimes
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

def convert_utc_columns_to_local(df: pd.DataFrame) -> None:
    utc_cols = [col for col in df.columns if col.endswith('_utc')]

    for col in utc_cols:
        # Ensure UTC-aware datetimes
        df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

        # Convert to local time and drop timezone info
        new_col = col[:-4]  # remove "_utc"
        df[new_col] = df[col].dt.tz_convert(None)

        # Drop original column
        df.drop(columns=[col], inplace=True)

