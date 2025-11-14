import os
import pandas as pd
from datetime import datetime, timezone

from utils.general import sequence_mgmt as sm
from utils.general import time_tools as tt

# --------------------------
# Log append function
# --------------------------

def start_log_module_run(local_config, entry):
    log_name = 'module_run'
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

    # Prepare fields
    new_row = {
        "DW_ModuleRunId": new_id,
        "DW_JobRunId": entry.get("DW_JobRunId"),
        "MsgLevel": entry.get("MsgLevel"),
        "ModuleType": entry.get("ModuleType"),
        "TimeStarted_utc": datetime.now(timezone.utc),
        "TimeEnded_utc": pd.NaT,       # Use pd.NaT for missing datetimes
        "Duration": 'na',             # Use pd.NA for missing string
        "Status": entry.get("Status"),
        "TasksTotal": int(0),
        "TasksSucceeded": int(0),
        "TasksFailed": int(0),
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

def end_log_module_run(local_config, update):
    log_name = 'module_run'
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

    id_val = update["DW_ModuleRunId"]

    if id_val not in df["DW_ModuleRunId"].values:
        raise ValueError(f"Log entry Id {id_val} not found.")

    # Update row
    idx = df.index[df["DW_ModuleRunId"] == id_val][0]

    df.at[idx, "MsgLevel"] = update.get("MsgLevel", df.at[idx, "MsgLevel"])
    df["TimeEnded_utc"] = pd.to_datetime(df["TimeEnded_utc"], utc=True)
    df.at[idx, "TimeEnded_utc"] = datetime.now(timezone.utc)
    df.at[idx, "TasksTotal"] = int(update.get("TasksTotal"))
    df.at[idx, "TasksSucceeded"] = int(update.get("TasksSucceeded"))
    df.at[idx, "TasksFailed"] = int(update.get("TasksFailed"))
    df.at[idx, "Status"] = update.get("Status", df.at[idx, "Status"])
    df.at[idx, "Message"] = update.get("Message", df.at[idx, "Message"])

    # Calculate Duration
    try:
        start_time = pd.to_datetime(df.at[idx, "TimeStarted_utc"])
        end_time = pd.to_datetime(df.at[idx, "TimeEnded_utc"])
        duration_str = tt.duration_xhxxmxxs(start_time,end_time)
        df["Duration"] = df["Duration"].astype("object")
        df.at[idx, "Duration"] = duration_str
    except Exception:
        df.at[idx, "Duration"] = None

    # Keep only rows where TimeStarted_utc is within the last 60 days
    log_retention_days = local_config.get('LOG_RETENTION_DAYS')
    df['TimeStarted_utc'] = pd.to_datetime(df['TimeStarted_utc'], utc=True)
    cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=log_retention_days)
    df = df[df['TimeStarted_utc'] >= cutoff]

    df.to_csv(log_path, index=False)
