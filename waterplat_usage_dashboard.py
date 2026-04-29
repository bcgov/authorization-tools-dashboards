"""
waterplat_usage_dashboard.py

Water Plat Tool Usage Dashboard - Static HTML Generator

Placeholder. The dashboard structure and metrics will be implemented in a
follow-up. For now this script writes a "Coming Soon" page so the multi-
dashboard publishing pipeline (subpath hosting + landing page) can be
verified end-to-end.

When implemented, this script will read JSONL logs from:
    s3://gssgeodrive/authorizations/logs/water_tool_logs/

Author:
    Moez Labiadh - GeoBC (moez.labiadh@gov.bc.ca)
"""

import os
from datetime import datetime, timezone

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output", "waterplat")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "index.html")

generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Water Plat Dashboard — Coming Soon</title>
    <style>
        body {{
            margin: 0;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            background: #1a1a2e;
            color: #e2e8f0;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }}
        .card {{
            background: #16213e;
            padding: 3rem 4rem;
            border-radius: 12px;
            text-align: center;
            max-width: 520px;
        }}
        h1 {{ color: #38bdf8; margin: 0 0 0.5rem; }}
        p  {{ color: #94a3b8; line-height: 1.6; }}
        a  {{ color: #4ade80; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .stamp {{ font-size: 0.85rem; margin-top: 1.5rem; }}
    </style>
</head>
<body>
    <div class="card">
        <h1>Water Plat Dashboard</h1>
        <p>Coming soon. Metrics for the Water Plat tool are under development.</p>
        <p><a href="../index.html">← Back to Authorization Tools Dashboards</a></p>
        <p class="stamp">Last build: {generated_at}</p>
    </div>
</body>
</html>
"""

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(HTML)

print(f"✓ Generated {OUTPUT_FILE}")
