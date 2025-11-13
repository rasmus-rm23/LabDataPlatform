import os
import shutil
import pandas as pd
from datetime import datetime

def get_file_list(folder='',extention=None):
    results = []

    for file in os.listdir(folder):
        if (extention is not None) and (not file.lower().endswith(f'.{extention}')):
            continue

        filepath = os.path.join(folder, file)
        results.append({
            "FileName": file
            , "FilePath": filepath
            , "DatetimeModified": datetime.fromtimestamp(os.path.getmtime(filepath))
        })

    df = pd.DataFrame(results)
    return df

def move_input_files_to_folder(file_path, is_consumed):
    folder_path = os.path.dirname(file_path)
    failed_dir = os.path.join(folder_path,'failed')
    consumed_dir = os.path.join(folder_path,'consumed')

    # Make sure destination folders exist
    os.makedirs(failed_dir, exist_ok=True)
    os.makedirs(consumed_dir, exist_ok=True)

    if is_consumed:
        dst_dir = consumed_dir
    else:
        dst_dir = failed_dir

    # Keep same filename
    dst = os.path.join(dst_dir, os.path.basename(file_path))

    try:
        shutil.move(file_path, dst)
        error_flag = False
        error_msg = 'No error'
    except Exception as e:
        error_flag = True
        error_msg = f"move_input_files_to_folder>> Failed to move {file_path}: {e}"

    return error_flag, error_msg