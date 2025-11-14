import os
from datetime import datetime, timezone
import pandas as pd

from utils.logs import log_task_run as ltr
from utils.general import files_handle as fh
from utils.general import dataframe_mgmt as dm
from utils.general import tables_mgmt as tm

def import_mylab_myjournal2(local_config,module_run_id):
    # Register Task start
    print('\nImporting MyJournal2 (MyLab)')

############ Find file list ############
    entry = {
        "DW_ModuleRunId": module_run_id,
        "MsgLevel": "INFO",
        "TaskType": "Read MyLab/MyJournal2 file list",
        "Status": "Started",
        "Message": "Started"
    }
    task_run_id = ltr.start_log_task_run(local_config,entry)

    folder_path = os.path.join(
        local_config.get('LANDINGZONE_ROOT_PATH'),
        'MyLab',
        'MyJournal2'
    )
    files_df = fh.get_file_list(folder=folder_path,extention='xlsx')
    n_files = files_df.shape[0]

    entry = {
        "DW_TaskRunId": task_run_id,
        "Status": "Completed",
        "Message": f"Number of files found: {n_files}"
    }
    ltr.end_log_task_run(local_config,entry)

    no_tasks_succeded = 1
    no_tasks_failed = 0

############ Define table and fields ############
    database_name = 'bronze_dsa'
    schema_name = 'mylab'
    table_name = 'myjournal2'
    obs_table_name = 'myjournal2_obs'
    key_col = 'NK_JournalID'

    # Single fields
    field_def_vec = [
        {
            'key': 'NK_JournalID',
            'field_name': 'NK_JournalID',
            'direction': 'right',
            'distance': 1,
            'mandatory': True
        },
        {
            'key': 'abc',
            'field_name': 'Abc',
            'direction': 'right',
            'distance': 1,
            'mandatory': False
        },
        {
            'key': 'AbcDown',
            'field_name': 'AbcDown',
            'direction': 'down',
            'distance': 1,
            'mandatory': False
        },
        {
            'key': 'Temperature (target)',
            'field_name': 'TempTarget',
            'direction': 'right',
            'distance': 1,
            'mandatory': False
        },
        {
            'key': 'Time (target)',
            'field_name': 'TimeTarget',
            'direction': 'right',
            'distance': 1,
            'mandatory': False
        },
        {
            'key': 'Colour',
            'field_name': 'Colour',
            'direction': 'right',
            'distance': 1,
            'mandatory': False
        },
        {
            'key': 'Product type',
            'field_name': 'Product category',
            'direction': 'right',
            'distance': 1,
            'mandatory': False
        }
    ]

    table_def_dict = {
        'table_header': ['Time','Temperature','Note']
        , 'column_names': ['Time (s)','Temperature (C)','Note']
    }


############ Loop through file list to import journal data ############
    for index, row in files_df.iterrows():
        # log task start
        entry = {
            "DW_ModuleRunId": module_run_id,
            "MsgLevel": "INFO",
            "TaskType": f"Import MyLab/MyJournal",
            "Status": "Started",
            "Message": f"Started on file '{row['FileName']}'"
        }
        task_run_id = ltr.start_log_task_run(local_config,entry)

        # Read file into dataframe

        df, error_flag, error_msg = fh.read_excel_safe(row['FilePath'])
        if error_flag:
            file_never_opened = True
        else:
            file_never_opened = False

        # Extract mapped single-fieelds
        if not error_flag:
            df_extract, error_flag, error_msg = dm.extract_single_fields(df,field_def_vec)

        # Upsert single-field values into staging table
        if not error_flag:
            no_fields_found = df_extract.shape[1]
            df_extract["ImportFileName"] = row['FileName']
            df_extract["TimeUpdated_utc"] = datetime.now(timezone.utc)
            df_extract["DW_TaskRunId"] = task_run_id

            error_flag, error_msg = tm.upsert_csv(local_config, database_name, schema_name, table_name, df_extract, key_col)

        # Extract defined sub table        
        if not error_flag:
            extracted_table_df, error_flag, error_msg = dm.extract_sub_table(df,table_def_dict)

        # upsert sub table values into database table
        if (not error_flag) and (extracted_table_df is not None):
            no_rows_found = extracted_table_df.shape[0]
            
            extracted_table_df["NK_JournalID"] = df_extract["NK_JournalID"].values[0]
            extracted_table_df["ImportFileName"] = row['FileName']
            extracted_table_df["TimeUpdated_utc"] = datetime.now(timezone.utc)
            extracted_table_df["DW_TaskRunId"] = task_run_id

            error_flag, error_msg = tm.upsert_csv(local_config, database_name, schema_name, obs_table_name, extracted_table_df, key_col)

        if not file_never_opened:
            error_flag_mv, error_msg_mv = fh.move_input_files_to_folder(file_path=row['FilePath'],is_consumed=not error_flag)
        else:
            error_flag_mv = False
            error_msg_mv = ''

        # prepare ending task log entry
        if error_flag or error_flag_mv:
            no_tasks_failed += 1
            if error_flag and (not error_flag_mv):
                msg = f"Error file '{row['FileName']}': {error_msg}"
            elif error_flag_mv and (not error_flag):
                msg = f"Error file '{row['FileName']}': {error_msg_mv}"
            else:
                msg = f"Error file '{row['FileName']}': {error_msg} AND {error_msg_mv}"

            entry = {
                "DW_TaskRunId": task_run_id
                , "MsgLevel": "WARNING"
                , "Status": "Failed"
                , "Message": msg
            }
        else:
            no_tasks_succeded += 1
            entry = {
                "DW_TaskRunId": task_run_id
                , "MsgLevel": "INFO"
                , "Status": "Completed"
                , "Message": f"Found in file '{row['FileName']}': #SimpleFields = {no_fields_found} / #ObsRows = {no_rows_found}"
            }

        ltr.end_log_task_run(local_config,entry)

    if no_tasks_failed == 0:
        info = {
            'Status': 'Success'
            , 'Message': f'All {no_tasks_succeded} tasks completed successfully'
        }
    else:
        info = {
            'Status': 'Warning'
            , 'Message': f'Of {no_tasks_succeded + no_tasks_failed} tasks, {no_tasks_failed} failed.'
        }

    return info, no_tasks_succeded, no_tasks_failed

