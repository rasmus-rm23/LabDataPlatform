import json
import time

from utils.logs import log_job_run as ljr

from t1dsa import master_import as mi
from t3reporting import master_reporting as mr

def execute_data_flow(local_config):
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
    print(module_summary)

    # Execute ETL programme: DSA to EDW
    # Dimensional modelling to be added later...

    # Build reports
    print('\n######### Build reports #########')
    module_summary = mr.run_master_reporting(local_config,run_id)
    print(module_summary)
    time.sleep(2)

    # Register MasterJob end
    print('\n######### Register MasterJob end #########')
    entry = {
        "JobRunId": run_id,
        # "MsgLevel": "WARNING" # Optional
        "Status": "Completed",
        "Message": "Complete dataflow ran successfully."
    }

    ljr.end_log_job_run(local_config,entry)
    # Build log reports
    print('\n######### Build log reports #########')

    return


if __name__ == "__main__":
    # Load config
    config_path = 'local_config.json'
    with open(config_path, "r") as f:
        config = json.load(f)

    execute_data_flow(config)

    print('\nDataflow execution complete')
