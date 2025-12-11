import time

from utils.logs import log_module_run as lmr
from t3reporting.report_templates import home_html as hh
from t3reporting.report_templates import navbar_create as nc
from t3reporting.mylab_reporting import report_mylab_myjournal as rmm

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

    no_tasks_succeeded = 0
    no_tasks_failed = 0

    nc.generate_navbar_html(json_file="t3reporting/report_templates/navbar_setup.json",local_config=local_config)
    no_tasks_succeeded += 1

    hh.generate_html_home(local_config=local_config)
    no_tasks_succeeded += 1

    if rmm.create_report_mylab_myjournal1(local_config):
        no_tasks_succeeded += 1
    else:
        no_tasks_failed += 1

    if rmm.create_report_mylab_myjournal2(local_config):
        no_tasks_succeeded += 1
    else:
        no_tasks_failed += 1

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

