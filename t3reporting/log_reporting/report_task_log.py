import pandas as pd

from utils.general import tables_mgmt as tm

from t3reporting.report_templates import one_table_html as ot

def create_report_log_task(local_config):
    df, error_flag, error_msg = tm.read_table_csv(local_config, 'meta_data','log','task_run')
    if error_flag:
        print('Error creating Task log html report')
        return
    
    df = df.sort_values(by='TimeStarted_utc',ascending=False)
    df = df.head(200)

    ot.generate_html_show_table(local_config,df,'log_task')