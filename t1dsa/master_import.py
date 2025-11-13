import time

from utils.logs import log_job_run as ljr
from utils.logs import log_module_run as lmr

from t1dsa import import_generic_journals as igj
from t1dsa.Mylab import import_mylab_myjournal as imlmj
from t1dsa.Mylab import import_mylab_myjournal2 as imlmj2

def run_master_import(local_config,job_run_id):
    # Register MasterJob start
    entry = {
        "JobRunId": job_run_id,
        "MsgLevel": "INFO",
        "ModuleType": "Master DSA Importer",
        "Status": "Started",
        "Message": "Master DSA Importer started"
    }
    module_run_id = lmr.start_log_module_run(local_config,entry)

    no_tasks_succeeded = 0
    no_tasks_failed = 0

    info, no_success, no_failed = imlmj.import_mylab_myjournal(local_config,module_run_id)
    no_tasks_succeeded += no_success
    no_tasks_failed += no_failed
    print(info['Message'])

    info, no_success, no_failed = imlmj2.import_mylab_myjournal2(local_config,module_run_id)
    no_tasks_succeeded += no_success
    no_tasks_failed += no_failed
    print(info['Message'])

    no_tasks_total = no_tasks_succeeded + no_tasks_failed
    entry = {
        "ModuleRunId": module_run_id,
        "MsgLevel": "INFO",
        "Status": "Completed",
        "TasksTotal": no_tasks_total,
        "TasksSucceeded": no_tasks_succeeded,
        "TasksFailed": no_tasks_failed,
        "Message": "Master Importer ran successfully."
    }
    if no_tasks_failed > 0:
        entry['MsgLevel'] = 'WARNING'
        entry['Message'] = f'Of {no_tasks_total} tasks, {no_tasks_failed} failed.'
    lmr.end_log_module_run(local_config,entry)

    return entry

