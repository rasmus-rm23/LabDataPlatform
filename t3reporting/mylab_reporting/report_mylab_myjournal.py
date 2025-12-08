import pandas as pd

from utils.general import tables_mgmt as tm

from t3reporting.report_templates import one_table_html as ot

def create_report_mylab_myjournal1(local_config):
    df, error_flag, error_msg = tm.read_table_csv(local_config, 'bronze_dsa','mylab','myjournal')
    if error_flag:
        print('Error creating MyLab.MyJournal html report')
        return False
    
    df = df.sort_values(by='NK_JournalID',ascending=False)

    ot.generate_html_show_table(local_config,df,'journals/mylab/myjournal','Journal 1 (from Mylab)')

    return True

def create_report_mylab_myjournal2(local_config):
    df, error_flag, error_msg = tm.read_table_csv(local_config, 'bronze_dsa','mylab','myjournal2')
    if error_flag:
        print('Error creating MyLab.MyJournal html report')
        return False
    
    df = df.sort_values(by='NK_JournalID',ascending=False)
    x_column = 'AbcDown'
    y_column = 'TempTarget'
    hue_column = 'TimeTarget'

    ot.generate_html_plot_and_table(local_config,df,'journals/mylab/myjournal2','Journal 2 (from Mylab)', x_column, y_column, hue_column=hue_column)

    return True

