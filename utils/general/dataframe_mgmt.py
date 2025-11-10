import json
import os
import pandas as pd

def extract_single_fields(df, field_def_vec):
    error_flag = False
    error_msg = 'No error'
    results = {}

    # Directions as (row_offset, col_offset)
    offsets = {
        'right':  (0, 1),
        'left':   (0, -1),
        'down':   (1, 0),
        'up':     (-1, 0)
    }

    for fd in field_def_vec:
        key = fd['key']
        field_name = fd['field_name']
        direction = fd['direction'].lower()
        distance = fd['distance']
        mandatory = fd.get('mandatory', False)

        # Find all locations where df == key
        positions = list(zip(*((df == key).to_numpy()).nonzero()))

        if not positions:
            if mandatory:
                raise ValueError(f"Mandatory key '{key}' not found in dataframe.")
            else:
                results[field_name] = None
                continue

        # Use the first match found
        row, col = positions[0]

        # Compute the target cell
        row_offset, col_offset = offsets[direction]
        target_row = row + row_offset * distance
        target_col = col + col_offset * distance

        # Check bounds
        if (0 <= target_row < df.shape[0]) and (0 <= target_col < df.shape[1]):
            value = df.iat[target_row, target_col]
        else:
            if mandatory:
                error_flag = True
                error_msg = f"Mandatory field '{field_name}' out of bounds from key '{key}'."

            value = None

        results[field_name] = value

    extracted_fields_df = pd.DataFrame([results])

    return extracted_fields_df, error_flag, error_msg

import pandas as pd

def extract_sub_table(df: pd.DataFrame, table_def_dict: dict) -> pd.DataFrame:
    error_flag = False
    error_msg = 'No error'

    table_header = table_def_dict["table_header"]
    column_names = table_def_dict["column_names"]
    header_len = len(table_header)

    df_str = df.astype(str)

    header_row = None
    header_col = None

    # --- 1. Find header anywhere in the dataframe ---
    for r_idx, row in df_str.iterrows():
        for c_idx in range(len(row) - header_len + 1):
            if list(row[c_idx:c_idx + header_len]) == table_header:
                header_row = r_idx
                header_col = c_idx
                break
        if header_row is not None:
            break

    if header_row is None:
        raise ValueError("Header row not found in dataframe.")

    # --- 2. Collect rows below until blank row encountered ---
    data_rows = []
    for r_idx in range(header_row + 1, len(df)):
        row = df.iloc[r_idx, header_col:header_col + header_len]

        # detect empty row (all empty/NaN)
        if row.isna().all() or all(str(x).strip() == "" for x in row):
            break

        data_rows.append(list(row))

    # No data case
    if not data_rows:
        return pd.DataFrame(columns=column_names)

    # --- 3. Build the result dataframe ---
    extracted_table_df = pd.DataFrame(data_rows, columns=column_names)

    return extracted_table_df, error_flag, error_msg


if __name__ == '__main__':
    config_path = 'local_config.json'
    with open(config_path, "r") as f:
        local_config = json.load(f)

    field_def_vec = [
        {
            'key': 'abc',
            'field_name': 'Abc',
            'direction': 'right',
            'distance': 1,
            'mandatory': True
        },
        {
            'key': 'AbcDown',
            'field_name': 'AbcDown',
            'direction': 'down',
            'distance': 1,
            'mandatory': False
        }
    ]

    file_name = 'test_2'
    file_path = os.path.join(
        local_config.get('LANDINGZONE_ROOT_PATH'),
        'MyLab',
        'MyJournal2',
        f'{file_name}.xlsx'
    )

    df = pd.read_excel(file_path)
    print(df.info())

    df_extract, error_flag, error_msg= extract_single_fields(df,field_def_vec)

    print(df_extract.info())
    print(df_extract)

    table_def_dict = {
        'table_header': ['Time','Temperature','Note']
        , 'column_names': ['Time (s)','Temperature (C)','Note']
    }
    extracted_table_df, error_flag, error_msg = extract_sub_table(df,table_def_dict)

    print(f'\nExtract table:')
    print(extracted_table_df)

    