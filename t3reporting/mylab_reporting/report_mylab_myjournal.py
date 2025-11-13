import pandas as pd

from utils.general import tables_mgmt as tm

from t3reporting.report_templates import one_table_html as ot

def create_report_mylab_myjournal_simple(local_config):
    df, error_flag, error_msg = tm.read_table_csv(local_config, 'bronze_dsa','mylab','myjournal')
    if error_flag:
        print('Error creating MyLab.MyJournal html report')
        return
    
    df = df.sort_values(by='NK_JournalID',ascending=False)

    ot.generate_html_show_table(local_config,df,'mylab_myjournal1')

def create_report_mylab_myjournal_hue_plot(local_config):
    df, error_flag, error_msg = tm.read_table_csv(local_config, 'bronze_dsa','mylab','myjournal')
    if error_flag:
        print('Error creating MyLab.MyJournal html report')
        return
    
    df = df.sort_values(by='NK_JournalID',ascending=False)

    ot.generate_html_plot_and_table(local_config,df,'mylab_myjournal1', x_column, y_column, hue_column=None, classifier_column=None)


