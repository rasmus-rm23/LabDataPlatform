import os
import pandas as pd
import matplotlib.pyplot as plt

def create_scatter_plot(local_config, df, report_name, x_column, y_column, hue_column):
    # Create scatter plot
    plt.figure(figsize=(8, 6))
    scatter = plt.scatter(
        df[x_column], df[y_column],
        c=df[hue_column],            # hue by column 'c'
        cmap='viridis',       # choose a nice colormap
        alpha=0.8,            # transparency for better visibility
        edgecolors='k'        # thin black border
    )

    # Add colorbar and labels
    plt.colorbar(scatter, label=hue_column)
    plt.xlabel(x_column)
    plt.ylabel(y_column)
    # plt.title('Scatter Plot of a vs b colored by c')
    plt.tight_layout()

    # Save to file
    save_file_path = os.path.join(
        local_config.get('REPORTING_ROOT_PATH'),
        f'{report_name}.html'
    )
    folder_path = os.path.dirname(save_file_path)
    os.makedirs(folder_path, exist_ok=True)
    plot_file_path = f'{report_name}.png'

    plt.savefig(save_file_path, dpi=300)
    plt.close()

    return plot_file_path

def create_grouped_scatter_plot(local_config, df, report_name, x_column, y_column, classifier_column):
    """
    Create a scatter plot of x_column vs y_column, with points colored
    by the categorical values in classifier_column. Saves the plot to file.
    """
    plt.figure(figsize=(8, 6))

    # Get unique categories and assign a color to each
    unique_classes = df[classifier_column].unique()
    colors = plt.cm.tab10(range(len(unique_classes)))  # up to 10 colors; use another colormap if needed

    for color, cls in zip(colors, unique_classes):
        subset = df[df[classifier_column] == cls]
        plt.scatter(
            subset[x_column],
            subset[y_column],
            label=str(cls),
            color=color,
            alpha=0.8,
            edgecolors='k'
        )

    plt.xlabel(x_column)
    plt.ylabel(y_column)
    plt.legend(title=classifier_column, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    # Save to file
    save_file_path = os.path.join(
        local_config.get('REPORTING_ROOT_PATH'),
        f'{report_name}.html'
    )
    folder_path = os.path.dirname(save_file_path)
    os.makedirs(folder_path, exist_ok=True)
    plot_file_path = f'{report_name}.png'

    plt.savefig(save_file_path, dpi=300, bbox_inches='tight')
    plt.close()

    return plot_file_path

def generate_html_show_table(local_config, df, report_name):
    # Generate HTML with embedded JS
    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>CSV Table Viewer</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 6px;
                text-align: left;
            }}
            th {{
                cursor: pointer;
                background-color: #f2f2f2;
            }}
            input {{
                width: 95%;
                box-sizing: border-box;
            }}
        </style>
    </head>
    <body>
        <h2>CSV Data Table</h2>
        <table id="dataTable">
            <thead>
                <tr>
                    {''.join(f'<th onclick="sortTable({i})">{col}</th>' for i, col in enumerate(df.columns))}
                </tr>
                <tr>
                    {''.join(f'<th><input type="text" onkeyup="filterTable({i})" placeholder="Filter {col}"></th>' for i, col in enumerate(df.columns))}
                </tr>
            </thead>
            <tbody>
                {''.join("<tr>" + "".join(f"<td>{row[col]}</td>" for col in df.columns) + "</tr>" for _, row in df.iterrows())}
            </tbody>
        </table>
    
    <script>
    function filterTable(colIndex) {{
        var input = document.querySelectorAll("thead input")[colIndex];
        var filter = input.value.toLowerCase();
        var table = document.getElementById("dataTable");
        var trs = table.getElementsByTagName("tbody")[0].getElementsByTagName("tr");
    
        for (var i = 0; i < trs.length; i++) {{
            var td = trs[i].getElementsByTagName("td")[colIndex];
            if (td) {{
                var txtValue = td.textContent || td.innerText;
                trs[i].style.display = txtValue.toLowerCase().indexOf(filter) > -1 ? "" : "none";
            }}
        }}
    }}
    
    function sortTable(n) {{
        var table = document.getElementById("dataTable");
        var rows = Array.from(table.rows).slice(2); // skip header + filter row
        var asc = table.getAttribute("data-sortdir") !== "asc";
    
        rows.sort(function(a, b) {{
            var A = a.cells[n].innerText.trim();
            var B = b.cells[n].innerText.trim();
            return asc ? A.localeCompare(B) : B.localeCompare(A);
        }});
    
        // Reattach rows in new order
        rows.forEach(function(row) {{
            table.tBodies[0].appendChild(row);
        }});
    
        // Toggle sort direction globally
        table.setAttribute("data-sortdir", asc ? "asc" : "desc");
    }}
    </script>

    </body>
    </html>
    """

    # Write HTML to file
    file_path = os.path.join(
        local_config.get('REPORTING_ROOT_PATH'),
        f'{report_name}.html'
    )
    folder_path = os.path.dirname(file_path)
    os.makedirs(folder_path, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_template)

    print(f"HTML file generated: {report_name}")

def generate_html_plot_and_table(local_config, df, report_name, x_column, y_column, hue_column=None, classifier_column=None):
    if hue_column is not None:
        plot_file_path = create_scatter_plot(local_config, df, report_name, x_column, y_column, hue_column=hue_column)
    elif classifier_column is not None:
        plot_file_path = create_grouped_scatter_plot(local_config, df, report_name, x_column, y_column, classifier_column=classifier_column)
    else:
        print(f'ERROR in generate_html_plot_and_table: Input hue_columns or classifier_column must be not None when generating report {report_name}')
        return

    # Generate HTML with embedded JS
    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>CSV Table Viewer</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 6px;
                text-align: left;
            }}
            th {{
                cursor: pointer;
                background-color: #f2f2f2;
            }}
            input {{
                width: 95%;
                box-sizing: border-box;
            }}
        </style>
    </head>
    <body>
        <figure style="text-align: center;">
            <img src="{plot_file_path}" alt="Scatter plot of a vs b" width="600">
            <figcaption>Figure 1: Scatter plot of column a vs b colored by column c</figcaption>
        </figure>

        <h2>CSV Data Table</h2>
        <table id="dataTable">
            <thead>
                <tr>
                    {''.join(f'<th onclick="sortTable({i})">{col}</th>' for i, col in enumerate(df.columns))}
                </tr>
                <tr>
                    {''.join(f'<th><input type="text" onkeyup="filterTable({i})" placeholder="Filter {col}"></th>' for i, col in enumerate(df.columns))}
                </tr>
            </thead>
            <tbody>
                {''.join("<tr>" + "".join(f"<td>{row[col]}</td>" for col in df.columns) + "</tr>" for _, row in df.iterrows())}
            </tbody>
        </table>
    
    <script>
    function filterTable(colIndex) {{
        var input = document.querySelectorAll("thead input")[colIndex];
        var filter = input.value.toLowerCase();
        var table = document.getElementById("dataTable");
        var trs = table.getElementsByTagName("tbody")[0].getElementsByTagName("tr");
    
        for (var i = 0; i < trs.length; i++) {{
            var td = trs[i].getElementsByTagName("td")[colIndex];
            if (td) {{
                var txtValue = td.textContent || td.innerText;
                trs[i].style.display = txtValue.toLowerCase().indexOf(filter) > -1 ? "" : "none";
            }}
        }}
    }}
    
    function sortTable(n) {{
        var table = document.getElementById("dataTable");
        var rows = Array.from(table.rows).slice(2); // skip header + filter row
        var asc = table.getAttribute("data-sortdir") !== "asc";
    
        rows.sort(function(a, b) {{
            var A = a.cells[n].innerText.trim();
            var B = b.cells[n].innerText.trim();
            return asc ? A.localeCompare(B) : B.localeCompare(A);
        }});
    
        // Reattach rows in new order
        rows.forEach(function(row) {{
            table.tBodies[0].appendChild(row);
        }});
    
        // Toggle sort direction globally
        table.setAttribute("data-sortdir", asc ? "asc" : "desc");
    }}
    </script>

    </body>
    </html>
    """

    # Write HTML to file
    file_path = os.path.join(
        local_config.get('REPORTING_ROOT_PATH'),
        f'{report_name}.html'
    )
    folder_path = os.path.dirname(file_path)
    os.makedirs(folder_path, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_template)

    print(f"HTML file generated: {report_name}")

