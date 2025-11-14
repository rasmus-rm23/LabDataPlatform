import json

from utils.logs import log_job_run as ljr

from t1dsa import master_import as mi
from t3reporting import master_reporting as mr
from t3reporting.log_reporting import report_job_log as rjl
from t3reporting.log_reporting import report_module_log as rml
from t3reporting.log_reporting import report_task_log as rtl

def execute_data_flow(local_config):
    warning_encountered = False

    # Register MasterJob start
    print('\n######### Register MasterJob start #########')
    entry = {
        "MsgLevel": "INFO",
        "JobType": "Complete dataflow",
        "Status": "Started",
        "Message": "Complete dataflow started"
    }
    run_id = ljr.start_log_job_run(local_config,entry)

    # Execute Landing Zone to DSA import
    print('\n######### Execute Landing Zone to DSA import #########')
    module_summary = mi.run_master_import(local_config,run_id)
    if module_summary['MsgLevel'] == 'WARNING':
        warning_encountered = True

    # Execute ETL programme: DSA to EDW
    # Dimensional modelling to be added later...

    # Build reports
    print('\n######### Build reports #########')
    module_summary = mr.run_master_reporting(local_config,run_id)
    if module_summary['MsgLevel'] == 'WARNING':
        warning_encountered = True

    # Register MasterJob end
    print('\n######### Register MasterJob end #########')
    if warning_encountered:
        entry = {
            "DW_JobRunId": run_id,
            "MsgLevel": "WARNING",
            "Status": "Completed",
            "Message": "Some jobs returned a warning"
        }
    else:
        entry = {
            "DW_JobRunId": run_id,
            "Status": "Completed",
            "Message": "Complete dataflow ran successfully."
        }

    ljr.end_log_job_run(local_config,entry)
    # Build log reports
    print('\n######### Build log reports #########')
    rjl.create_report_log_job(local_config)
    rml.create_report_log_module(local_config)
    rtl.create_report_log_task(local_config)

    return


if __name__ == "__main__":
    # Load config
    config_path = 'local_config.json'
    with open(config_path, "r") as f:
        config = json.load(f)

    execute_data_flow(config)

    print('\nDataflow execution complete')
