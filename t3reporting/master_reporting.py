import time

from utils.logs import log_job_run as ljr
from utils.logs import log_module_run as lmr

def run_master_reporting(local_config,job_run_id):
    # Register MasterJob start
    entry = {
        "DW_JobRunId": job_run_id,
        "MsgLevel": "INFO",
        "ModuleType": "Master Reporter",
        "Status": "Started",
        "Message": "Master Reporter started"
    }
    module_run_id = lmr.start_log_module_run(local_config,entry)

    no_tasks_succeeded = 3
    no_tasks_failed = 1


    no_tasks_total = no_tasks_succeeded + no_tasks_failed
    entry = {
        "DW_ModuleRunId": module_run_id,
        "MsgLevel": "INFO",
        "Status": "Completed",
        "TasksTotal": no_tasks_total,
        "TasksSucceeded": no_tasks_succeeded,
        "TasksFailed": no_tasks_failed,
        "Message": "Master Reporter ran successfully."
    }
    lmr.end_log_module_run(local_config,entry)

    return entry

