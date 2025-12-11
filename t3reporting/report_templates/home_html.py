import os
import pandas as pd

def generate_html_home(local_config):
    html_header = f"""
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
            }}
        </style>
    """

    html_body = f"""
        <br>
        <h2>Data Platform main page</h2>
        <br>

        Use top menu to navigate to the needed information.
        <br>
        <br>
        Maybe add dynamic content here....
        <br>
    </html>
    """

    master_file_path = os.path.join(
        local_config.get('REPORTING_ROOT_PATH'),
        f'templates/master.html'
    )

    # Read master template
    with open(master_file_path, "r", encoding="utf-8") as f:
        html_template = f.read()

    # Replace placeholders
    html_template = (
        html_template
            .replace("HEADER_CONTENT_PLACE_HOLDER", html_header)
            .replace("BODY_CONTENT_PLACE_HOLDER", html_body)
    )

    # Write HTML to file
    file_path = os.path.join(
        local_config.get('REPORTING_ROOT_PATH'),
        f'index.html'
    )
    folder_path = os.path.dirname(file_path)
    os.makedirs(folder_path, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_template)

