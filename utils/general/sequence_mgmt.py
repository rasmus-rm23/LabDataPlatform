import os
import time
import pandas as pd

def sequence_get_next_id(local_config, database_name, sequence_name, retries=10, delay=1):
    """
    Reads/updates a simple CSV file storing just one integer ID.
    Acts like an auto-increment identity column.
    Retries when the file is locked or used by another process.
    """
    seq_path = os.path.join(
        local_config.get('DATABASE_ROOT_PATH')
        ,database_name
        ,'seq'
        ,f'{sequence_name}.csv'
    )

    os.makedirs(os.path.dirname(seq_path), exist_ok=True)

    attempt = 0
    while attempt < retries:
        try:
            # --- READ CURRENT ID ---
            if os.path.exists(seq_path):
                df = pd.read_csv(seq_path,header=None)
                current_id = int(df.iloc[0, 0])
            else:
                current_id = 0

            next_id = current_id + 1

            # --- WRITE UPDATED ID ---
            pd.DataFrame([next_id]).to_csv(seq_path, index=False, header=False)

            return next_id

        except Exception as e:
            attempt += 1
            if attempt >= retries:
                raise RuntimeError(
                    f"Failed to update ID file after {retries} attempts. Last error: {e}"
                )
            time.sleep(delay)

    # Should never reach here
    raise RuntimeError("Unexpected error in _get_next_id")
