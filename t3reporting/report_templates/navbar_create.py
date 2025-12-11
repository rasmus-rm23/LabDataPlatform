import json
import os
from pathlib import Path


def generate_navbar_html(
    json_file: str,
    local_config
):
    """
    Generate a full HTML file containing a Bootstrap navbar
    based on a nested JSON menu definition.
    """
    brand_name = local_config.get("PLATFORM_NAME")
    report_base_path = local_config.get("REPORTING_ROOT_PATH")

    with open(json_file, "r", encoding="utf-8") as f:
        menu = json.load(f)

    def render_items(items, level=0):
        html = []

        for i, item in enumerate(items):
            text = item.get("display text", "")
            link = item.get("link", "#")
            subitems = item.get("subitems")

            if subitems:
                dropdown_id = f"dropdown_{level}_{i}"

                if level == 0:
                    html.append(f"""
                    <li class="nav-item dropdown">
                        <a class="nav-link dropdown-toggle"
                           href="{link or '#'}"
                           id="{dropdown_id}"
                           role="button"
                           data-bs-toggle="dropdown"
                           aria-expanded="false">
                           {text}
                        </a>
                        <ul class="dropdown-menu">
                            {render_items(subitems, level + 1)}
                        </ul>
                    </li>
                    """)
                else:
                    html.append(f"""
                    <li class="dropdown-submenu">
                        <a class="dropdown-item dropdown-toggle"
                           href="{link or '#'}">
                           {text}
                        </a>
                        <ul class="dropdown-menu">
                            {render_items(subitems, level + 1)}
                        </ul>
                    </li>
                    """)
            else:
                cls = "nav-link" if level == 0 else "dropdown-item"
                html.append(f"""
                <li class="{'nav-item' if level == 0 else ''}">
                    <a class="{cls}" href="{link}">{text}</a>
                </li>
                """)

        return "\n".join(html)

    navbar_html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>{brand_name}</title>

    <!-- Bootstrap 5 -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"
          rel="stylesheet">

    <style>
        /* Enable multi-level dropdowns */
        .dropdown-submenu {{
            position: relative;
        }}

        .dropdown-submenu > .dropdown-menu {{
            top: 0;
            left: 100%;
            margin-left: 0.1rem;
        }}

        .dropdown-submenu:hover > .dropdown-menu {{
            display: block;
        }}
    </style>

    HEADER_CONTENT_PLACE_HOLDER
</head>
<body>

<nav class="navbar navbar-expand-lg navbar-dark bg-primary">
    <div class="container-fluid">
        <a class="navbar-brand" href="#">{brand_name}</a>

        <button class="navbar-toggler"
                type="button"
                data-bs-toggle="collapse"
                data-bs-target="#navbarNav"
                aria-controls="navbarNav"
                aria-expanded="false"
                aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
        </button>

        <div class="collapse navbar-collapse" id="navbarNav">
            <ul class="navbar-nav mx-auto">
                {render_items(menu)}
            </ul>
        </div>
    </div>
</nav>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>

BODY_CONTENT_PLACE_HOLDER

</body>
</html>
"""

    navbar_html = navbar_html.replace("<report_base_path>", report_base_path)

    file_path = os.path.join(
        local_config.get('REPORTING_ROOT_PATH'),
        f'templates/master.html'
    )
    only_path = os.path.dirname(file_path)
    os.makedirs(only_path, exist_ok=True)

    Path(file_path).write_text(navbar_html, encoding="utf-8")

if __name__ == "__main__":
    config_path = 'local_config.json'
    with open(config_path, "r") as f:
        config = json.load(f)

    generate_navbar_html(
        json_file="t3reporting/report_templates/navbar_setup.json",
        local_config=config
    )