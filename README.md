# Authorization Tools Dashboards

Usage statistics dashboards BC GeoBC authorization tools. Dashboards are automatically updated nightly from tool usage logs stored in NRS object storage and published as a static site via GitHub Pages.

## Live Dashboards

🔗 **[Landing page](https://bcgov.github.io/authorization-tools-dashboards/)**

| Dashboard | URL |
|---|---|
| Legal Description Schedule (LDS) Tool| [/lds/](https://bcgov.github.io/authorization-tools-dashboards/lds/) |
| Water Plat Tool | [/waterplat/](https://bcgov.github.io/authorization-tools-dashboards/waterplat/) |

## What They Track

- **Usage Volume** — weekly run trends, runs by region, GIS vs Non-GIS user breakdown
- **Performance & Reliability** — execution times, success and failure rates, weekly failure rate trends, common error messages
- **Feature Adoption** — usage rates for optional features


## How It Works

```
S3 Object Storage          GitHub Actions (nightly)         GitHub Pages
┌──────────────┐          ┌─────────────────────┐          ┌──────────────────┐
│  JSONL logs  │──read──▶ │  Python scripts     │──push──▶│  Static HTML     │
│  per tool    │          │  generate per-tool  │          │  served at       │
│  (OS bucket) │          │  HTML with Plotly   │          │  /<tool>/ paths  │
└──────────────┘          └─────────────────────┘          └──────────────────┘
```

## Data Sources

The dashboards read JSONL log files from the NRS ObjectStore:

| Tool | S3 prefix | File patterns |
|---|---|---|
| LDS | `authorizations/logs/lds_tool_logs/` | `*_summary.jsonl`, `*_detail.jsonl` |
| Water Plat | `authorizations/logs/water_tool_logs/` | `*_summary.jsonl` |

## Repository Structure

```
├── .github/workflows/
│   └── update_dashboard.yml          # Nightly GitHub Actions workflow
├── landing/
│   └── index.html                    # Landing page (copied to output/index.html during build)
├── lds_usage_dashboard.py            # LDS dashboard generator
├── waterplat_usage_dashboard.py      # Water Plat dashboard generator
├── requirements.txt                  # Python dependencies
└── README.md
```

The published `output/` tree (in the `gh-pages` branch) looks like:

```
output/
├── index.html              # landing page
├── lds/index.html          # LDS dashboard
└── waterplat/index.html    # Water Plat dashboard
```

## Local Development

Set the three S3 environment variables, then run either generator:

```bash
export S3_NRS_ENDPOINT=...
export S3_GSS_GEODRIVE_KEY_ID=...
export S3_GSS_GEODRIVE_SECRET_KEY=...

python lds_usage_dashboard.py        # writes output/lds/index.html
python waterplat_usage_dashboard.py  # writes output/waterplat/index.html
cp landing/index.html output/index.html
```

Open `output/index.html` in a browser to preview.
