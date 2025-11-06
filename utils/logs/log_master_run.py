import os
import pandas as pd
from datetime import datetime, timezone

from utils.general import sequence_mgmt as sm
from utils.general import time_tools as tt

# --------------------------
# Log append function
# --------------------------

def append_log_master_run(local_config, entry):
    """
    Appends a new row to the log file.
    entry = {
        'Level': ...,
        'Type': ...,
        'Status': ...,
        'Message': ...
    }
    Returns: Id (integer)
    """
    log_name = 'master_run'
    database_name = 'meta_data'
    log_path = os.path.join(
        local_config.get('DATABASE_ROOT_PATH')
        ,database_name
        ,'log'
        ,f'{log_name}.csv'
    )

    # Ensure directory exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Generate next ID
    new_id = sm.sequence_get_next_id(local_config,database_name,log_name)

    # Convert fields
    new_row = {
        "Id": new_id,
        "Level": entry.get("Level"),
        "Type": entry.get("Type"),
        "TimeStarted_utc": datetime.now(timezone.utc),
        "TimeEnded_utc": None,
        "Duration": None,
        "Status": entry.get("Status"),
        "Message": entry.get("Message"),
    }

    # Append row to log
    if os.path.exists(log_path):
        df = pd.read_csv(log_path)
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        df = pd.DataFrame([new_row])

    df.to_csv(log_path, index=False)

    return new_id


# --------------------------
# Log update function
# --------------------------

def update_log_entry(local_config, update):
    """
    Updates an existing row:
    update = {
        'Id': ...,
        'Status': ...,
        'Message': ...
    }
    Automatically calculates Duration.
    """
    log_name = 'master_run'
    database_name = 'meta_data'
    log_path = os.path.join(
        local_config.get('DATABASE_ROOT_PATH')
        ,database_name
        ,'log'
        ,f'{log_name}.csv'
    )
    if not os.path.exists(log_path):
        raise FileNotFoundError("Log file does not exist.")

    df = pd.read_csv(log_path)

    id_val = update["Id"]

    if id_val not in df["Id"].values:
        raise ValueError(f"Log entry Id {id_val} not found.")

    # Update row
    idx = df.index[df["Id"] == id_val][0]

    df.at[idx, "TimeEnded_utc"] = datetime.now(timezone.utc)
    df.at[idx, "Status"] = update.get("Status", df.at[idx, "Status"])
    df.at[idx, "Message"] = update.get("Message", df.at[idx, "Message"])

    # Calculate Duration
    try:
        start_time = pd.to_datetime(df.at[idx, "TimeStarted_utc"])
        end_time = pd.to_datetime(df.at[idx, "TimeEnded_utc"])
        # start_time = df.at[idx, "TimeStarted_utc"]
        # end_time = df.at[idx, "TimeEnded_utc"]
        duration_str = tt.duration_xhxxmxxs(start_time,end_time)
        print(duration_str)
        df.at[idx, "Duration"] = duration_str
    except Exception:
        df.at[idx, "Duration"] = None

    df.to_csv(log_path, index=False)
