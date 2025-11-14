import time

from utils.logs import log_task_run as ltr

def import_generic_journals(local_config,module_run_id):
    # Register Task start
    print('\nImport generic journals - NOT IMPLEMENTED')
    entry = {
        "DW_ModuleRunId": module_run_id,
        "MsgLevel": "INFO",
        "ModuleType": "Import generic xlsx journals",
        "Status": "Started",
        "Message": "Import generic xlsx journals started"
    }
    task_run_id = ltr.start_log_task_run(local_config,entry)

    entry = {
        "DW_TaskRunId": task_run_id,
        # "MsgLevel": "WARNING" # Optional
        "Status": "Completed",
        "Message": "Import generic xlsx journals complete"
    }
    ltr.end_log_task_run(local_config,entry)

    no_tasks_success = 1
    no_tasks_failed = 0
    return entry, no_tasks_success, no_tasks_failed

