import time

from utils.logs import log_job_run as ljr
from utils.logs import log_module_run as lmr

def run_master_import(local_config,job_run_id):
    # Register MasterJob start
    print('\n#### Register MasterModule start ####')
    entry = {
        "JobRunId": job_run_id,
        "MsgLevel": "INFO",
        "ModuleType": "Master DSA Importer",
        "Status": "Started",
        "Message": "Master DSA Importer started"
    }
    module_run_id = lmr.start_log_module_run(local_config,entry)

    time.sleep(2)

    no_tasks_succeeded = 7
    no_tasks_failed = 2
    no_tasks_total = no_tasks_succeeded + no_tasks_failed
    print('\n#### Register MasterModule end ####')
    entry = {
        "ModuleRunId": module_run_id,
        # "MsgLevel": "WARNING" # Optional
        "Status": "Completed",
        "TasksTotal": no_tasks_total,
        "TasksSucceeded": no_tasks_succeeded,
        "TasksFailed": no_tasks_failed,
        "Message": "Master Importer ran successfully."
    }
    lmr.end_log_module_run(local_config,entry)

    return entry

