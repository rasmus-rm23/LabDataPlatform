import pandas as pd
import os
import time
import csv

def write_table_csv(local_config, database_name, schema_name, table_name, df, retries=10, delay=2):
    table_path = os.path.join(
        local_config.get('DATABASE_ROOT_PATH'),
        database_name,
        schema_name,
        f'{table_name}.csv'
    )

    error_flag = False
    error_msg = 'No error'
    
    for attempt in range(1, retries + 1):
        try:
            os.makedirs(os.path.dirname(table_path), exist_ok=True)
            df.to_csv(table_path, index=False, quoting=csv.QUOTE_MINIMAL)
            break  # success
        except Exception as e:
            if attempt == retries:
                error_flag = True
                error_msg = f'Unable to write table: {e}'
            else:
                time.sleep(delay)

    return error_flag, error_msg


def read_table_csv(local_config, database_name, schema_name, table_name, retries=10, delay=2):
    table_path = os.path.join(
        local_config.get('DATABASE_ROOT_PATH'),
        database_name,
        schema_name,
        f'{table_name}.csv'
    )

    if not os.path.exists(table_path):
        return None, True, 'Table does not exist!'

    df = None
    error_flag = False
    error_msg = 'No error'

    for attempt in range(1, retries + 1):
        try:
            df = pd.read_csv(table_path)
            break  # success
        except Exception as e:
            if attempt == retries:
                error_flag = True
                error_msg = f'Unable to read table: {e}'
            else:
                time.sleep(delay)

    return df, error_flag, error_msg

def upsert_csv(local_config, database_name, schema_name, table_name, df, key_col, retries=10, delay=2): # df: pd.DataFrame, filename: str, key_col: str = "NK_JournalID"):
    table_path = os.path.join(
        local_config.get('DATABASE_ROOT_PATH'),
        database_name,
        schema_name,
        f'{table_name}.csv'
    )

    existing_df, error_flag, error_msg = read_table_csv(local_config, database_name, schema_name, table_name, retries=10, delay=2)
    if not error_flag:
        try:
            # Drop duplicates based on key_col
            existing_df[key_col] = existing_df[key_col].astype(str).str.strip()
            df[key_col] = df[key_col].astype(str).str.strip()
            existing_df = existing_df[~existing_df[key_col].isin(df[key_col])]
            # concatenate existing and new data
            updated_df = pd.concat([existing_df, df], ignore_index=True)
        except Exception as e:
            error_flag = True
            error_msg = f'Unable to upsert: {e}'
    else:
        updated_df = df.copy()
        error_flag = False

    if not error_flag:
        error_flag, error_msg = write_table_csv(local_config, database_name, schema_name, table_name, updated_df, retries, delay)

    return error_flag, error_msg
