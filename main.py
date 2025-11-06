import json
import time

from utils.logs import log_master_run as lmr

def execute_data_flow(local_config):
    # Register MasterJob start
    print('\n######### Register MasterJob start #########')
    entry = {
        "Level": "INFO",
        "Type": "Task",
        "Status": "Started",
        "Message": "Complte run started"
    }
    run_id = lmr.append_log_master_run(local_config,entry)

    # Execute Landing Zone to DSA import
    print('\n######### Execute Landing Zone to DSA import #########')
    time.sleep(5)

    # Execute ETL programme: DSA to EDW
    # Dimensional modelling to be added later...

    # Build reports
    print('\n######### Build reports #########')

    # Register MasterJob end
    print('\n######### Register MasterJob end #########')
    entry = {
        "Id": run_id,
        "Status": "Completed",
        "Message": "Step 1 completed."
    }

    lmr.update_log_entry(local_config,entry)
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
