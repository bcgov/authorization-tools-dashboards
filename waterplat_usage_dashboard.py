"""
waterplat_usage_dashboard.py

Water Plat Tool Usage Dashboard - Static HTML Generator

Reads all monthly JSONL files matching *_summary.jsonl and *_detail.jsonl
patterns from the NRS ObjectStore (water_tool_logs prefix).

Detail logs are joined by run_id to enrich error messages beyond what the
summary captures.

Author:
    Moez Labiadh - GeoBC (moez.labiadh@gov.bc.ca)
"""

import os
import io
import re
import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# =============================================================================
# CONFIGURATION
# =============================================================================
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output", "waterplat")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "index.html")

# S3-compatible object storage configuration
S3_BUCKET = "gssgeodrive"
S3_PREFIX = "authorizations/logs/water_tool_logs/"

s3_client = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_NRS_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_GSS_GEODRIVE_KEY_ID"),
    aws_secret_access_key=os.getenv("S3_GSS_GEODRIVE_SECRET_KEY"),
)

# =============================================================================
# USER CONFIGURATION
# =============================================================================
# Developer IDIR(s) to exclude from all stats (test runs)
EXCLUDED_USERS = {'MLABIADH'}

# GIS specialists — same list as LDS dashboard. Anyone else is "Non-GIS".
GIS_USERS = {'MSEASTWO', 'ALLSHEPH', 'SEPARSON', 'AERASMUS', 'JBUSSE',
             'JFOY', 'CSOSTAD', 'JSANDERS', 'SRAHIMI'}

GROUP_GIS = 'GIS Users'
GROUP_NON_GIS = 'Non-GIS Users'

# =============================================================================
# COLOR SCHEME
# =============================================================================
COLORS = {
    'bg_primary': '#1a1a2e',
    'bg_secondary': '#16213e',
    'accent': '#e94560',
    'success': '#4ade80',
    'warning': '#fbbf24',
    'error': '#f03737',
    'text': '#e2e8f0',
    'text_muted': '#94a3b8',
    'chart': ['#e94560', '#4ade80', '#fbbf24', '#38bdf8', '#a78bfa', '#fb923c']
}

# Stages to skip when extracting the "best" error from detail logs.
_GENERIC_ERROR_STAGES = {'completion', 'main'}

# Stage → color mapping for error charts. Unknown stages fall back to COLORS['error'].
STAGE_COLORS = {
    'initialization': '#303DBA',
    'admin_boundary_detection': COLORS['chart'][3],
    'workspace_creation': COLORS['chart'][5],
    'plat_generation': COLORS['chart'][4],
    'export': COLORS['chart'][2],
    'completion': COLORS['text_muted'],
}


# =============================================================================
# S3 HELPERS
# =============================================================================
def _list_s3_keys(suffix):
    """List all object keys under S3_PREFIX that end with the given suffix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                keys.append(obj["Key"])
    return sorted(keys)


def _read_jsonl_from_s3(key):
    """Download a JSONL file from S3 and return a DataFrame."""
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    body = response["Body"].read()
    return pd.read_json(io.BytesIO(body), lines=True)


# =============================================================================
# DATA LOADING
# =============================================================================
def load_data():
    """Load data from all monthly JSONL files in S3."""

    summary_keys = _list_s3_keys("_summary.jsonl")

    if not summary_keys:
        print(f"! No summary JSONL files found under s3://{S3_BUCKET}/{S3_PREFIX}")
        return pd.DataFrame(), pd.DataFrame()

    print(f"Found {len(summary_keys)} summary file(s)")

    summary_dfs = []
    for key in summary_keys:
        try:
            df_temp = _read_jsonl_from_s3(key)
            filename = key.rsplit("/", 1)[-1]
            print(f"  ✓ Loaded {len(df_temp)} records from {filename}")
            summary_dfs.append(df_temp)
        except Exception as e:
            filename = key.rsplit("/", 1)[-1]
            print(f"  ! Error loading {filename}: {e}")

    if not summary_dfs:
        print("! No summary data loaded")
        return pd.DataFrame(), pd.DataFrame()

    df_summary = pd.concat(summary_dfs, ignore_index=True)
    print(f"✓ Total summary records: {len(df_summary)}")

    df_summary['timestamp_start'] = pd.to_datetime(df_summary['timestamp_start'])
    df_summary['date'] = df_summary['timestamp_start'].dt.date
    df_summary['hour'] = df_summary['timestamp_start'].dt.hour

    detail_keys = _list_s3_keys("_detail.jsonl")
    print(f"\nFound {len(detail_keys)} detail file(s)")

    detail_dfs = []
    for key in detail_keys:
        try:
            df_temp = _read_jsonl_from_s3(key)
            filename = key.rsplit("/", 1)[-1]
            print(f"  ✓ Loaded {len(df_temp)} records from {filename}")
            detail_dfs.append(df_temp)
        except Exception as e:
            filename = key.rsplit("/", 1)[-1]
            print(f"  ! Error loading {filename}: {e}")

    df_detail = pd.concat(detail_dfs, ignore_index=True) if detail_dfs else pd.DataFrame()
    if not df_detail.empty:
        print(f"✓ Total detail records: {len(df_detail)}")

    return df_summary, df_detail


def enrich_errors_from_detail(df_summary, df_detail):
    """
    Join detail-level error info onto the summary dataframe.

    For each run_id, the detail log may contain multiple ERROR-level records
    across different stages. Picks the most informative error per run, prefers
    specific stages over generic ones (completion/main), and fills in error
    messages for runs the summary missed entirely.
    """
    if df_detail.empty:
        df_summary['detail_error_message'] = df_summary.get('error_message')
        df_summary['detail_error_stage'] = None
        return df_summary

    errors = df_detail[df_detail['level'] == 'ERROR'].copy()

    if errors.empty:
        df_summary['detail_error_message'] = df_summary.get('error_message')
        df_summary['detail_error_stage'] = None
        return df_summary

    errors['is_generic'] = errors['stage'].isin(_GENERIC_ERROR_STAGES)
    errors = errors.sort_values(['run_id', 'is_generic', 'timestamp'])
    best_errors = errors.groupby('run_id').first().reset_index()

    best_errors['detail_error_message'] = best_errors['message'].apply(_clean_error_message)
    best_errors['detail_error_stage'] = best_errors['stage']

    df_summary = df_summary.merge(
        best_errors[['run_id', 'detail_error_message', 'detail_error_stage']],
        on='run_id',
        how='left'
    )

    mask_no_detail = df_summary['detail_error_message'].isna()
    if 'error_message' in df_summary.columns:
        df_summary.loc[mask_no_detail, 'detail_error_message'] = df_summary.loc[
            mask_no_detail, 'error_message'
        ]

    n_enriched = df_summary['detail_error_message'].notna().sum()
    n_summary_only = (
        df_summary['error_message'].notna().sum()
        if 'error_message' in df_summary.columns else 0
    )
    n_new = (
        df_summary['detail_error_message'].notna()
        & (df_summary.get('error_message', pd.Series([None]*len(df_summary))).isna())
    ).sum()
    print(f"\n✓ Error enrichment: {n_enriched} runs with errors "
          f"({n_summary_only} from summary, +{n_new} additional from detail logs)")

    return df_summary


def enrich_geography_from_file_num(df):
    """
    Propagate geography fields (region, water_district, etc.) from successful
    runs to failure records of the same file_num.

    Why: failures often happen before the admin_boundary_detection stage runs,
    so the summary record for a failed run is missing region/water_district/
    watershed/mapsheet. Multiple runs against the same authorization file share
    the same physical location, so we can recover the geography from any other
    run of the same file_num that did get past admin detection.
    """
    if 'file_num' not in df.columns:
        return df

    geo_cols = ['region', 'water_district', 'water_precinct',
                'land_district', 'watershed', 'mapsheet']

    filled_total = 0
    for col in geo_cols:
        if col not in df.columns:
            continue
        # Build file_num -> value lookup from rows where this column is known
        # and the value isn't a "NONE" sentinel.
        known = df[df[col].notna()].copy()
        known = known[known[col].astype(str).str.upper() != 'NONE']
        if known.empty:
            continue
        lookup = (
            known.drop_duplicates('file_num', keep='first')
                 .set_index('file_num')[col]
                 .to_dict()
        )
        mask = df[col].isna() & df['file_num'].notna()
        if not mask.any():
            continue
        filled = df.loc[mask, 'file_num'].map(lookup)
        n_filled = int(filled.notna().sum())
        if n_filled:
            df.loc[mask, col] = filled
            filled_total += n_filled

    if filled_total:
        print(f"✓ Geography enrichment: filled {filled_total} missing field(s) "
              f"on failure records via file_num lookup")
    return df


def _clean_error_message(msg):
    """
    Normalize error messages for grouping. Strips noisy prefixes, truncates
    multi-line stack traces, and caps length for chart readability.
    """
    if not isinstance(msg, str):
        return msg

    msg = re.sub(r'^Unexpected Exception:\s*', '', msg)
    msg = re.sub(r'^Failed to execute\.\s*Parameters are not valid\.\s*', '', msg)

    if '\n' in msg:
        msg = msg.split('\n')[0].strip()

    if len(msg) > 75:
        msg = msg[:72] + '...'

    return msg.strip()


# =============================================================================
# METRICS CALCULATION
# =============================================================================
def clean_username(username):
    """Remove IDIR\\ prefix from username and normalize to uppercase."""
    if isinstance(username, str):
        return username.replace('IDIR\\', '').replace('IDIR/', '').upper()
    return username


def assign_user_group(clean_user):
    """Assign user to GIS or Non-GIS group."""
    if isinstance(clean_user, str) and clean_user.upper() in GIS_USERS:
        return GROUP_GIS
    return GROUP_NON_GIS


def calculate_metrics(df):
    """Calculate all metrics from dataframe."""
    total = len(df)
    if total == 0:
        return {}

    date_min = df['timestamp_start'].min().strftime('%Y-%m-%d')
    date_max = df['timestamp_start'].max().strftime('%Y-%m-%d')

    error_col = 'detail_error_message' if 'detail_error_message' in df.columns else 'error_message'
    has_error = df[error_col].notna() & (df[error_col].astype(str).str.strip().str.len() > 0)

    gis_runs = int((df['user_group'] == GROUP_GIS).sum())
    non_gis_runs = int((df['user_group'] == GROUP_NON_GIS).sum())

    median_duration = float(df['duration_seconds'].median())
    p90_duration = float(df['duration_seconds'].quantile(0.90))

    def adoption_rate(col):
        if col not in df.columns:
            return 0.0
        return float((df[col] > 0).sum()) / total * 100

    def flag_true_rate(col):
        if col not in df.columns:
            return 0.0
        return float((df[col] == True).sum()) / total * 100

    def non_null_rate(col):
        if col not in df.columns:
            return 0.0
        return float(df[col].notna().sum()) / total * 100

    return {
        'total_runs': total,
        'unique_machines': int(df['machine'].nunique()),
        'unique_users': int(df['clean_user'].nunique()),
        'gis_users': int(df.loc[df['user_group'] == GROUP_GIS, 'clean_user'].nunique()),
        'non_gis_users': int(df.loc[df['user_group'] == GROUP_NON_GIS, 'clean_user'].nunique()),
        'gis_runs': gis_runs,
        'non_gis_runs': non_gis_runs,
        'unique_licences': int(df['lic_num'].nunique()) if 'lic_num' in df.columns else 0,
        'unique_files': int(df['file_num'].nunique()) if 'file_num' in df.columns else 0,
        'median_duration': median_duration,
        'p90_duration': p90_duration,
        'success_rate': float((df['status'] == 'success').sum()) / total * 100,
        'error_rate': float(has_error.sum()) / total * 100,
        'warning_rate': (
            float((df['warning_count'] > 0).sum()) / total * 100
            if 'warning_count' in df.columns else 0.0
        ),
        'error_types': int(df.loc[has_error, error_col].nunique()),
        'non_gis_success_rate': (
            float(((df['user_group'] == GROUP_NON_GIS) & (df['status'] == 'success')).sum())
            / max(non_gis_runs, 1) * 100
        ),
        'non_gis_error_rate': (
            float(has_error[df['user_group'] == GROUP_NON_GIS].sum())
            / max(non_gis_runs, 1) * 100
        ),
        'date_from': date_min,
        'date_to': date_max,
        # Layer adoption
        'pod_adoption': adoption_rate('pod_count'),
        'pid_adoption': adoption_rate('pid_count'),
        'well_adoption': adoption_rate('well_tag_count'),
        'mineral_adoption': adoption_rate('mineral_count'),
        'range_adoption': adoption_rate('range_count'),
        'su_adoption': adoption_rate('su_count'),
        # Optional flag rates
        'uot_rate': flag_true_rate('uot'),
        'temp_pd_rate': flag_true_rate('temp_pd_provided'),
        'temp_st_rate': flag_true_rate('temp_st_provided'),
        'scale_override_rate': non_null_rate('scale_override'),
    }


# =============================================================================
# CHART CREATION
# =============================================================================
def get_chart_layout(title="", height=300):
    """Return consistent chart layout."""
    return {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'color': COLORS['text'], 'family': 'system-ui, -apple-system, sans-serif', 'size': 12},
        'margin': {'l': 60, 'r': 30, 't': 50, 'b': 60},
        'title': {'text': title, 'font': {'size': 14, 'color': COLORS['text_muted']}},
        'xaxis': {'gridcolor': 'rgba(255,255,255,0.1)', 'zerolinecolor': 'rgba(255,255,255,0.1)'},
        'yaxis': {'gridcolor': 'rgba(255,255,255,0.1)', 'zerolinecolor': 'rgba(255,255,255,0.1)'},
        'height': height,
        'autosize': True,
    }


def _empty_fig(title, message="No data available", height=300):
    fig = go.Figure()
    fig.add_annotation(text=message, xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
    fig.update_layout(**get_chart_layout(title, height=height))
    return fig


# ----------------------------- SECTION A: VOLUME -----------------------------

def create_weekly_trend(df):
    """Weekly run counts split by GIS / Non-GIS, with current week dashed."""
    df_copy = df.copy()
    df_copy['week'] = pd.to_datetime(df_copy['date']).dt.to_period('W').apply(lambda r: r.start_time)
    weekly = df_copy.groupby(['week', 'user_group']).size().reset_index(name='runs')

    current_week = pd.Timestamp.now().to_period('W').start_time

    fig = go.Figure()
    for group, color in [(GROUP_GIS, COLORS['chart'][3]), (GROUP_NON_GIS, COLORS['chart'][5])]:
        grp = weekly[weekly['user_group'] == group].sort_values('week')
        completed = grp[grp['week'] < current_week]
        partial = grp[grp['week'] == current_week]

        fig.add_trace(go.Scatter(
            x=completed['week'], y=completed['runs'], mode='lines+markers',
            name=group, line=dict(color=color), marker=dict(color=color)
        ))

        if not partial.empty and not completed.empty:
            bridge_x = [completed['week'].iloc[-1], partial['week'].iloc[0]]
            bridge_y = [completed['runs'].iloc[-1], partial['runs'].iloc[0]]
            fig.add_trace(go.Scatter(
                x=bridge_x, y=bridge_y, mode='lines+markers',
                showlegend=False,
                line=dict(color=color, dash='dash'),
                marker=dict(color=color, symbol='circle-open', size=8)
            ))
        elif not partial.empty:
            fig.add_trace(go.Scatter(
                x=partial['week'], y=partial['runs'], mode='markers',
                showlegend=False,
                marker=dict(color=color, symbol='circle-open', size=8)
            ))

    fig.update_layout(**get_chart_layout('Weekly Run Trend'))
    fig.update_layout(
        yaxis_title='Total Runs',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
    )
    fig.add_annotation(
        text="Dashed = current week (partial)",
        xref="paper", yref="paper", x=1.0, y=-0.25,
        showarrow=False, font=dict(size=10, color=COLORS['text_muted']),
        xanchor='right'
    )
    return fig


def _user_distribution(df, user_group, title):
    """Top-10 users in a group, broken down by status (success/warning/error)."""
    grp_df = df[df['user_group'] == user_group]
    if grp_df.empty:
        return _empty_fig(title, message=f"No {user_group} runs", height=320)

    top_users = grp_df['clean_user'].value_counts().head(10).index.tolist()
    df_top = grp_df[grp_df['clean_user'].isin(top_users)].copy()

    def normalize_status(s):
        s = str(s).lower()
        if 'success' in s:
            return 'success'
        if 'warning' in s:
            return 'warning'
        return 'error'

    df_top['status_group'] = df_top['status'].apply(normalize_status)
    stats = df_top.groupby(['clean_user', 'status_group']).size().reset_index(name='count')
    color_map = {'success': COLORS['success'], 'error': COLORS['error'], 'warning': COLORS['warning']}

    fig = px.bar(stats, x='count', y='clean_user', color='status_group', orientation='h',
                 color_discrete_map=color_map,
                 category_orders={'clean_user': top_users})
    fig.update_layout(**get_chart_layout(title, height=320))
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        yaxis_title='user_idir',
        xaxis_title='Number of runs',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5, title=None),
        margin={'l': 60, 'r': 30, 't': 80, 'b': 60},
    )
    return fig


def create_user_distribution_gis(df):
    return _user_distribution(df, GROUP_GIS, 'Top 10 GIS Users')


def create_user_distribution_non_gis(df):
    return _user_distribution(df, GROUP_NON_GIS, 'Top 10 Non-GIS Users')


def create_region_distribution(df):
    """Bar by region, stacked by GIS/Non-GIS. Drops rows missing region."""
    df_copy = df.dropna(subset=['region']).copy() if 'region' in df.columns else pd.DataFrame()
    if df_copy.empty:
        return _empty_fig('Runs by Region')
    region_counts = df_copy.groupby(['region', 'user_group']).size().reset_index(name='count')
    color_map = {GROUP_GIS: COLORS['chart'][3], GROUP_NON_GIS: COLORS['chart'][5]}
    fig = px.bar(region_counts, x='count', y='region', orientation='h',
                 color='user_group', color_discrete_map=color_map, barmode='stack')
    fig.update_layout(**get_chart_layout('Runs by Region'))
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title='Number of runs',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5, title_text=''),
        margin={'l': 60, 'r': 30, 't': 80, 'b': 60},
    )
    return fig


def create_user_group_split(df):
    """Donut: GIS vs Non-GIS share of total runs."""
    group_counts = df['user_group'].value_counts().reset_index()
    group_counts.columns = ['group', 'count']
    color_map = {GROUP_GIS: COLORS['chart'][3], GROUP_NON_GIS: COLORS['chart'][5]}
    colors = [color_map.get(g, COLORS['text_muted']) for g in group_counts['group']]
    fig = px.pie(group_counts, values='count', names='group',
                 color_discrete_sequence=colors, hole=0.4)
    fig.update_layout(**get_chart_layout('Runs by User Group', height=320))
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def create_usage_heatmap(df):
    """Heatmap of run counts by day-of-week × hour, business hours Mon–Fri."""
    df_copy = df.copy()
    df_copy['day_of_week'] = df_copy['timestamp_start'].dt.day_name()
    df_copy['hour'] = df_copy['timestamp_start'].dt.hour

    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    df_copy = df_copy[df_copy['day_of_week'].isin(day_order)]
    df_copy = df_copy[df_copy['hour'].between(6, 18)]

    pivot = df_copy.groupby(['day_of_week', 'hour']).size().reset_index(name='runs')
    pivot = pivot.pivot(index='day_of_week', columns='hour', values='runs').fillna(0)
    pivot = pivot.reindex(day_order)

    for h in range(6, 19):
        if h not in pivot.columns:
            pivot[h] = 0
    pivot = pivot[sorted(pivot.columns)]

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=[f'{h}:00' for h in pivot.columns],
        y=pivot.index,
        colorscale=[[0, COLORS['bg_secondary']], [0.5, COLORS['warning']], [1, COLORS['accent']]],
        hovertemplate='%{y} at %{x}<br>Runs: %{z}<extra></extra>',
    ))
    fig.update_layout(**get_chart_layout('Peak Usage (Day × Hour)', height=320))
    fig.update_layout(
        xaxis_title='Hour of Day',
        yaxis=dict(autorange='reversed'),
    )
    return fig


# ------------------------ SECTION B: PERFORMANCE & RELIABILITY ------------------------

def create_status_distribution(df):
    """Donut: success vs failure (Non-GIS only)."""
    non_gis_df = df[df['user_group'] == GROUP_NON_GIS]
    if non_gis_df.empty:
        return _empty_fig('Status Distribution (Non-GIS)', height=320)
    status = non_gis_df['status'].value_counts().reset_index()
    status.columns = ['status', 'count']
    colors = [COLORS['success'] if s == 'success' else COLORS['error'] for s in status['status']]
    fig = px.pie(status, values='count', names='status', color_discrete_sequence=colors, hole=0.4)
    fig.update_layout(**get_chart_layout('Status Distribution (Non-GIS)', height=320))
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def create_failure_rate_trend(df):
    """Weekly failure-rate trend with regional context (Non-GIS only)."""
    error_col = 'detail_error_message' if 'detail_error_message' in df.columns else 'error_message'

    df_copy = df[df['user_group'] == GROUP_NON_GIS].copy()
    df_copy['week'] = df_copy['timestamp_start'].dt.to_period('W').apply(lambda r: r.start_time)
    df_copy['has_error'] = (
        df_copy[error_col].notna()
        & (df_copy[error_col].astype(str).str.strip().str.len() > 0)
    )

    weekly = df_copy.groupby('week').agg(
        total=('run_id', 'size'),
        errors=('has_error', 'sum'),
    ).reset_index()
    weekly['failure_rate'] = weekly['errors'] / weekly['total'] * 100
    weekly = weekly.sort_values('week')

    if weekly.empty or weekly['total'].sum() == 0:
        return _empty_fig('Weekly Failure Rate Trend (Non-GIS)')

    overall_avg = weekly['errors'].sum() / weekly['total'].sum() * 100
    weekly['ma'] = weekly['failure_rate'].rolling(3, center=True, min_periods=1).mean()

    # Region-level breakdown — drop rows missing region (failures often lack it)
    region_df = df_copy.dropna(subset=['region']) if 'region' in df_copy.columns else df_copy.iloc[0:0]
    if not region_df.empty:
        region_weekly = region_df.groupby(['week', 'region']).agg(
            total=('run_id', 'size'),
            errors=('has_error', 'sum'),
        ).reset_index()
        region_weekly['failure_rate'] = region_weekly['errors'] / region_weekly['total'] * 100
        region_weekly = region_weekly.sort_values(['region', 'week'])
        region_weekly['failure_rate'] = (
            region_weekly.groupby('region')['failure_rate']
            .transform(lambda s: s.rolling(3, center=True, min_periods=1).mean())
        )
        region_order = (
            region_df.loc[region_df['has_error'], 'region']
            .value_counts()
            .index.tolist()
        )
        for r in region_df['region'].unique():
            if r not in region_order:
                region_order.append(r)
        region_color_map = {
            region: COLORS['chart'][i % len(COLORS['chart'])]
            for i, region in enumerate(region_order)
        }
    else:
        region_weekly = pd.DataFrame()
        region_order = []
        region_color_map = {}

    fig = go.Figure()

    fig.add_hrect(
        y0=max(overall_avg - 2, 0), y1=overall_avg + 2,
        fillcolor=COLORS['text_muted'], opacity=0.08,
        line_width=0,
    )
    fig.add_hline(
        y=overall_avg,
        line_dash='dot', line_color=COLORS['text_muted'], line_width=1,
        annotation_text=f"Avg {overall_avg:.1f}%",
        annotation_position='top left',
        annotation_font=dict(size=11, color=COLORS['text_muted']),
    )

    for region in region_order:
        grp = region_weekly[region_weekly['region'] == region].sort_values('week')
        if grp.empty:
            continue
        fig.add_trace(go.Scatter(
            x=grp['week'], y=grp['failure_rate'],
            mode='lines',
            name=region,
            line=dict(color=region_color_map[region], width=1.7),
            opacity=0.5,
            hovertemplate=(
                f"<b>{region}</b><br>"
                "Week: %{x|%b %d}<br>"
                "Failure rate: %{y:.1f}%<br>"
                "<extra></extra>"
            ),
        ))

    fig.add_trace(go.Scatter(
        x=weekly['week'], y=weekly['failure_rate'],
        mode='lines+markers',
        name='Overall',
        line=dict(color=COLORS['text'], width=3),
        marker=dict(color=COLORS['text'], size=6),
        hovertemplate=(
            "<b>Overall</b><br>"
            "Week: %{x|%b %d}<br>"
            "Failure rate: %{y:.1f}%<br>"
            "<extra></extra>"
        ),
    ))

    fig.add_trace(go.Scatter(
        x=weekly['week'], y=weekly['ma'],
        mode='lines',
        name='3-wk trend',
        line=dict(color=COLORS['accent'], width=2.5, dash='dash'),
        hovertemplate=(
            "<b>3-week moving avg</b><br>"
            "Week: %{x|%b %d}<br>"
            "Trend: %{y:.1f}%<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(**get_chart_layout('Weekly Failure Rate Trend (Non-GIS)', height=350))
    fig.update_layout(
        yaxis_title='Failure Rate (%)',
        yaxis=dict(
            gridcolor='rgba(255,255,255,0.1)',
            zerolinecolor='rgba(255,255,255,0.1)',
            rangemode='tozero',
        ),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
    )
    return fig


def create_error_messages(df):
    """Top error messages bar chart, color-coded by stage (Non-GIS only)."""
    df = df[df['user_group'] == GROUP_NON_GIS]
    error_col = 'detail_error_message' if 'detail_error_message' in df.columns else 'error_message'
    stage_col = 'detail_error_stage' if 'detail_error_stage' in df.columns else None

    mask = df[error_col].notna() & (df[error_col].astype(str).str.strip().str.len() > 0)
    cols = [error_col, 'user_group']
    if stage_col:
        cols.append(stage_col)
    error_df = df.loc[mask, cols].copy()

    if len(error_df) == 0:
        return _empty_fig('Common Error Messages', message='No errors recorded', height=380)

    if stage_col:
        grouped = error_df.groupby(error_col).agg(
            count=(error_col, 'size'),
            stage=(stage_col, lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'unknown'),
        ).reset_index()
        grouped.columns = ['message', 'count', 'stage']
        grouped = grouped.sort_values('count', ascending=False).head(8)

        fig = go.Figure()
        for _, row in grouped.iterrows():
            color = STAGE_COLORS.get(row['stage'], COLORS['error'])
            fig.add_trace(go.Bar(
                y=[row['message']],
                x=[row['count']],
                orientation='h',
                marker_color=color,
                name=row['stage'],
                showlegend=False,
                hovertemplate=(
                    f"<b>{row['message']}</b><br>"
                    f"Stage: {row['stage']}<br>"
                    f"Count: {row['count']}"
                    f"<extra></extra>"
                ),
                text=[f"{row['count']}  [{row['stage']}]"],
                textposition='outside',
                textfont=dict(size=11),
            ))

        fig.update_layout(**get_chart_layout('Common Error Messages - Non-GIS (from Detail Logs)', height=380))
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            margin={'l': 250, 'r': 100, 't': 50, 'b': 60},
        )
    else:
        errors = error_df[error_col].value_counts().head(8).reset_index()
        errors.columns = ['message', 'count']
        fig = px.bar(errors, x='count', y='message', orientation='h',
                     color_discrete_sequence=[COLORS['error']])
        fig.update_layout(**get_chart_layout('Common Error Messages', height=380))

    return fig


def create_error_by_region(df):
    """Pie of error distribution by region (Non-GIS only)."""
    df = df[df['user_group'] == GROUP_NON_GIS]
    error_col = 'detail_error_message' if 'detail_error_message' in df.columns else 'error_message'
    mask = df[error_col].notna() & (df[error_col].astype(str).str.strip().str.len() > 0)
    error_df = df.loc[mask].copy()

    if 'region' not in error_df.columns:
        return _empty_fig('Errors by Region (Non-GIS)', height=320)

    error_df = error_df.dropna(subset=['region'])
    if len(error_df) == 0:
        return _empty_fig('Errors by Region (Non-GIS)', message='No errors with region', height=320)

    region_errors = error_df['region'].value_counts().reset_index()
    region_errors.columns = ['region', 'count']
    region_color_map = {
        region: COLORS['chart'][i % len(COLORS['chart'])]
        for i, region in enumerate(region_errors['region'])
    }
    fig = px.pie(region_errors, values='count', names='region',
                 color='region', color_discrete_map=region_color_map, hole=0.4)
    fig.update_layout(**get_chart_layout('Errors by Region (Non-GIS)', height=320))
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def create_error_stages(df):
    """Donut of errors by pipeline stage (Non-GIS only)."""
    df = df[df['user_group'] == GROUP_NON_GIS]
    stage_col = 'detail_error_stage' if 'detail_error_stage' in df.columns else None
    if stage_col is None:
        return _empty_fig('Top Error Stages (Non-GIS)', message='No stage data', height=320)

    error_col = 'detail_error_message'
    mask = df[error_col].notna() & (df[error_col].astype(str).str.strip().str.len() > 0)
    error_df = df.loc[mask].copy()

    if len(error_df) == 0 or error_df[stage_col].isna().all():
        return _empty_fig('Top Error Stages (Non-GIS)', message='No errors recorded', height=320)

    stage_counts = error_df[stage_col].value_counts().reset_index()
    stage_counts.columns = ['stage', 'count']
    colors = [STAGE_COLORS.get(s, COLORS['text_muted']) for s in stage_counts['stage']]

    fig = go.Figure(data=[go.Pie(
        labels=stage_counts['stage'],
        values=stage_counts['count'],
        hole=0.4,
        marker=dict(colors=colors),
    )])
    fig.update_layout(**get_chart_layout('Top Error Stages (Non-GIS)', height=320))
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


# --------------- SECTION C: AUTHORIZATION CONTEXT & FEATURE USAGE ---------------

def create_map_type_distribution(df):
    """Donut showing the distribution of map types. Buckets <2% slices into 'Other'."""
    if 'map_type' not in df.columns:
        return _empty_fig('Map Type Distribution', height=320)

    counts = df['map_type'].dropna().value_counts()
    if counts.empty:
        return _empty_fig('Map Type Distribution', height=320)

    total = counts.sum()
    threshold = 0.02 * total
    main = counts[counts >= threshold]
    other = counts[counts < threshold].sum()
    if other > 0:
        main = pd.concat([main, pd.Series({'Other': other})])

    data = main.reset_index()
    data.columns = ['map_type', 'count']
    colors = [COLORS['chart'][i % len(COLORS['chart'])] for i in range(len(data))]

    fig = px.pie(data, values='count', names='map_type',
                 color_discrete_sequence=colors, hole=0.4)
    fig.update_layout(**get_chart_layout('Map Type Distribution', height=320))
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def create_layer_adoption(df):
    """Horizontal bar: % of runs that include each ID type."""
    total = len(df)
    if total == 0:
        return _empty_fig('ID Type Usage', height=350)

    layers = [
        ('PODs', 'pod_count'),
        ('PIDs', 'pid_count'),
        ('Well Tags', 'well_tag_count'),
        ('Mineral Tenures', 'mineral_count'),
        ('Range Tenures', 'range_count'),
        ('Special Use Permits', 'su_count'),
    ]
    rows = []
    for label, col in layers:
        if col in df.columns:
            rate = float((df[col] > 0).sum()) / total * 100
        else:
            rate = 0.0
        rows.append({'feature': label, 'rate': rate})

    data = pd.DataFrame(rows)
    fig = px.bar(data, x='rate', y='feature', orientation='h',
                 color_discrete_sequence=[COLORS['chart'][3]],
                 text=[f"{v:.1f}%" for v in data['rate']])
    fig.update_layout(**get_chart_layout('ID Type Usage (% of runs)', height=350))
    fig.update_xaxes(title='Usage %', range=[0, 100])
    fig.update_traces(textposition='outside')
    fig.update_layout(margin={'l': 160, 'r': 60, 't': 50, 'b': 60})
    return fig


def create_optional_flags(df):
    """Horizontal bar: % of runs with each optional flag set."""
    total = len(df)
    if total == 0:
        return _empty_fig('Optional Features Usage', height=320)

    rows = []
    for label, col, mode in [
        ('UOT', 'uot', 'true'),
        ('Temp PD provided', 'temp_pd_provided', 'true'),
        ('Temp Storage provided', 'temp_st_provided', 'true'),
        ('Scale override', 'scale_override', 'non_null'),
    ]:
        if col not in df.columns:
            rate = 0.0
        elif mode == 'true':
            rate = float((df[col] == True).sum()) / total * 100
        else:
            rate = float(df[col].notna().sum()) / total * 100
        rows.append({'feature': label, 'rate': rate})

    data = pd.DataFrame(rows)
    fig = px.bar(data, x='rate', y='feature', orientation='h',
                 color_discrete_sequence=[COLORS['chart'][4]],
                 text=[f"{v:.1f}%" for v in data['rate']])
    fig.update_layout(**get_chart_layout('Optional Features Usage', height=320))
    fig.update_xaxes(title='Usage %', range=[0, 100])
    fig.update_traces(textposition='outside')
    fig.update_layout(margin={'l': 180, 'r': 60, 't': 50, 'b': 60})
    return fig


# =============================================================================
# HTML GENERATION
# =============================================================================
def generate_html(df, metrics):
    """Generate complete HTML dashboard."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    generated_at = datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m-%d %H:%M %Z')

    charts = {
        'weekly_trend': create_weekly_trend(df).to_html(full_html=False, include_plotlyjs=False),
        'user_dist_gis': create_user_distribution_gis(df).to_html(full_html=False, include_plotlyjs=False),
        'user_dist_non_gis': create_user_distribution_non_gis(df).to_html(full_html=False, include_plotlyjs=False),
        'region_dist': create_region_distribution(df).to_html(full_html=False, include_plotlyjs=False),
        'user_group_split': create_user_group_split(df).to_html(full_html=False, include_plotlyjs=False),
        'usage_heatmap': create_usage_heatmap(df).to_html(full_html=False, include_plotlyjs=False),
        'status_dist': create_status_distribution(df).to_html(full_html=False, include_plotlyjs=False),
        'failure_rate_trend': create_failure_rate_trend(df).to_html(full_html=False, include_plotlyjs=False),
        'error_msgs': create_error_messages(df).to_html(full_html=False, include_plotlyjs=False),
        'error_region': create_error_by_region(df).to_html(full_html=False, include_plotlyjs=False),
        'error_stages': create_error_stages(df).to_html(full_html=False, include_plotlyjs=False),
        'map_type_dist': create_map_type_distribution(df).to_html(full_html=False, include_plotlyjs=False),
        'layer_adoption': create_layer_adoption(df).to_html(full_html=False, include_plotlyjs=False),
        'optional_flags': create_optional_flags(df).to_html(full_html=False, include_plotlyjs=False),
    }

    def format_duration(seconds):
        if seconds >= 60:
            return f"{seconds / 60:.1f}m"
        return f"{seconds:.0f}s"

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Water Plat Tool Usage Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, {COLORS['bg_primary']} 0%, #0f0f1a 50%, {COLORS['bg_primary']} 100%);
            color: {COLORS['text']};
            min-height: 100vh;
            padding: 32px;
        }}
        header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 32px;
            padding-bottom: 20px;
            border-bottom: 1px solid rgba(56, 189, 248, 0.3);
        }}
        .header-left h1 {{
            font-size: 42px;
            font-weight: 700;
            letter-spacing: -1px;
            line-height: 1;
        }}
        .header-right {{ text-align: right; flex-shrink: 0; }}
        .header-right .meta-line {{
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 8px;
            margin-bottom: 4px;
        }}
        .header-right .meta-line:last-child {{ margin-bottom: 0; }}
        .status-indicator {{
            display: inline-block;
            width: 10px;
            height: 10px;
            background: {COLORS['success']};
            border-radius: 50%;
            box-shadow: 0 0 10px {COLORS['success']};
            animation: pulse 2s infinite;
            flex-shrink: 0;
        }}
        @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.5; }} }}
        .subtitle {{
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 3px;
            color: {COLORS['text_muted']};
            font-family: monospace;
        }}
        section {{ margin-bottom: 48px; }}
        .section-header {{
            font-size: 20px;
            text-transform: uppercase;
            letter-spacing: 4px;
            color: {COLORS['chart'][3]};
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid {COLORS['chart'][3]};
            font-family: monospace;
            font-weight: 700;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}
        .metric-card {{
            background: linear-gradient(135deg, {COLORS['bg_secondary']} 0%, {COLORS['bg_primary']} 100%);
            border: 1px solid rgba(56, 189, 248, 0.2);
            border-radius: 4px;
            padding: 20px;
        }}
        .metric-card .label {{
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 2px;
            color: {COLORS['text_muted']};
            margin-bottom: 8px;
            font-family: monospace;
        }}
        .metric-card .value {{
            font-size: 32px;
            font-weight: 700;
            color: {COLORS['text']};
            margin-bottom: 4px;
        }}
        .metric-card .card-subtitle {{
            font-size: 12px;
            color: {COLORS['text_muted']};
            font-family: monospace;
        }}
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
        }}
        .charts-grid-3 {{ grid-template-columns: repeat(3, 1fr); }}
        .chart-container {{
            background: linear-gradient(180deg, {COLORS['bg_secondary']} 0%, {COLORS['bg_primary']} 100%);
            border: 1px solid rgba(56, 189, 248, 0.15);
            border-radius: 4px;
            padding: 16px;
            overflow: hidden;
        }}
        .chart-container .js-plotly-plot {{ width: 100% !important; }}
        .chart-container .plotly {{ width: 100% !important; }}
        footer {{
            text-align: center;
            padding-top: 24px;
            border-top: 1px solid rgba(56, 189, 248, 0.2);
        }}
        footer p {{
            font-size: 11px;
            color: {COLORS['text_muted']};
            font-family: monospace;
            letter-spacing: 1px;
        }}
        footer a {{ color: {COLORS['chart'][1]}; text-decoration: none; }}
        footer a:hover {{ text-decoration: underline; }}
        @media (max-width: 1200px) {{
            .charts-grid-3 {{ grid-template-columns: 1fr 1fr; }}
        }}
        @media (max-width: 768px) {{
            body {{ padding: 16px; }}
            header {{ flex-direction: column; align-items: flex-start; gap: 12px; }}
            .header-right {{ text-align: left; }}
            .header-right .meta-line {{ justify-content: flex-start; }}
            .header-left h1 {{ font-size: 28px; }}
            .charts-grid, .charts-grid-3 {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <header>
        <div class="header-left">
            <h1>Water Plat Tool Usage Dashboard</h1>
        </div>
        <div class="header-right">
            <div class="meta-line">
                <span class="subtitle">Last Updated: {generated_at}</span>
                <span class="status-indicator"></span>
            </div>
            <div class="meta-line">
                <span class="subtitle">Data Period: {metrics['date_from']} to {metrics['date_to']}</span>
            </div>
        </div>
    </header>

    <!-- USAGE VOLUME -->
    <section>
        <h2 class="section-header">Usage Volume</h2>
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="label">Total Runs</div>
                <div class="value">{metrics['total_runs']}</div>
                <div class="card-subtitle">All records</div>
            </div>
            <div class="metric-card">
                <div class="label">Unique Users</div>
                <div class="value">{metrics['unique_users']}</div>
                <div class="card-subtitle">{metrics['gis_users']} GIS &bull; {metrics['non_gis_users']} Non-GIS</div>
            </div>
            <div class="metric-card">
                <div class="label">GIS Runs</div>
                <div class="value">{metrics['gis_runs']}</div>
                <div class="card-subtitle">GIS specialist runs</div>
            </div>
            <div class="metric-card">
                <div class="label">Non-GIS Runs</div>
                <div class="value">{metrics['non_gis_runs']}</div>
                <div class="card-subtitle">Non-GIS user runs</div>
            </div>
        </div>
        <div class="charts-grid charts-grid-3">
            <div class="chart-container">{charts['weekly_trend']}</div>
            <div class="chart-container">{charts['region_dist']}</div>
            <div class="chart-container">{charts['user_group_split']}</div>
        </div>
        <div style="display: grid; grid-template-columns: 1fr 1fr 0.7fr; gap: 16px; margin-top: 16px;">
            <div class="chart-container">{charts['user_dist_gis']}</div>
            <div class="chart-container">{charts['user_dist_non_gis']}</div>
            <div class="chart-container">{charts['usage_heatmap']}</div>
        </div>
    </section>

    <!-- PERFORMANCE & RELIABILITY -->
    <section>
        <h2 class="section-header">Performance &amp; Reliability</h2>
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="label">Median Run Time</div>
                <div class="value">{format_duration(metrics['median_duration'])}</div>
                <div class="card-subtitle">All runs</div>
            </div>
            <div class="metric-card">
                <div class="label">P90 Run Time</div>
                <div class="value">{format_duration(metrics['p90_duration'])}</div>
                <div class="card-subtitle">90th percentile</div>
            </div>
            <div class="metric-card">
                <div class="label">Success Rate</div>
                <div class="value">{metrics['non_gis_success_rate']:.1f}%</div>
                <div class="card-subtitle">Non-GIS runs</div>
            </div>
            <div class="metric-card">
                <div class="label">Failure Rate</div>
                <div class="value">{metrics['non_gis_error_rate']:.1f}%</div>
                <div class="card-subtitle">Non-GIS runs</div>
            </div>
            <div class="metric-card">
                <div class="label">Warning Rate</div>
                <div class="value">{metrics['warning_rate']:.1f}%</div>
                <div class="card-subtitle">Runs with warnings</div>
            </div>
            <div class="metric-card">
                <div class="label">Error Types</div>
                <div class="value">{metrics['error_types']}</div>
                <div class="card-subtitle">Unique errors</div>
            </div>
        </div>
        <div class="charts-grid" style="margin-top: 16px;">
            <div class="chart-container" style="grid-column: span 2;">{charts['failure_rate_trend']}</div>
        </div>
        <div class="charts-grid charts-grid-3" style="margin-top: 16px;">
            <div class="chart-container">{charts['status_dist']}</div>
            <div class="chart-container">{charts['error_region']}</div>
            <div class="chart-container">{charts['error_stages']}</div>
        </div>
        <div class="charts-grid" style="margin-top: 16px;">
            <div class="chart-container" style="grid-column: span 2;">{charts['error_msgs']}</div>
        </div>
    </section>

    <!-- FEATURE ADOPTION -->
    <section>
        <h2 class="section-header">Feature Adoption</h2>
        <div class="charts-grid charts-grid-3">
            <div class="chart-container">{charts['map_type_dist']}</div>
            <div class="chart-container">{charts['layer_adoption']}</div>
            <div class="chart-container">{charts['optional_flags']}</div>
        </div>
    </section>

    <footer>
        <p>Tool Usage Analytics &bull; Data from monthly JSONL logs (summary + detail) &bull; <a href="../index.html">Back to dashboards</a></p>
    </footer>

    <script>
        window.addEventListener('resize', function() {{
            document.querySelectorAll('.js-plotly-plot').forEach(function(plot) {{
                Plotly.Plots.resize(plot);
            }});
        }});
        window.addEventListener('load', function() {{
            setTimeout(function() {{
                document.querySelectorAll('.js-plotly-plot').forEach(function(plot) {{
                    Plotly.Plots.resize(plot);
                }});
            }}, 100);
        }});
    </script>
</body>
</html>'''

    return html


# =============================================================================
# MAIN
# =============================================================================
if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("Water Plat Tool Usage Dashboard - HTML Generator")
    print("=" * 60)

    df_summary, df_detail = load_data()

    if df_summary.empty:
        print("\n! No data loaded — writing placeholder page.")
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write("<!DOCTYPE html><html><body><h1>Water Plat Dashboard</h1>"
                    "<p>No data available yet.</p></body></html>")
        raise SystemExit(0)

    df_summary['clean_user'] = df_summary['user_os'].apply(clean_username)

    before = len(df_summary)
    df_summary = df_summary[~df_summary['clean_user'].isin(EXCLUDED_USERS)].copy()
    excluded = before - len(df_summary)
    if excluded > 0:
        print(f"\n✓ Excluded {excluded} developer test runs ({', '.join(EXCLUDED_USERS)})")

    df_summary['user_group'] = df_summary['clean_user'].apply(assign_user_group)
    gis_n = (df_summary['user_group'] == GROUP_GIS).sum()
    non_gis_n = (df_summary['user_group'] == GROUP_NON_GIS).sum()
    print(f"✓ User groups: {gis_n} GIS runs, {non_gis_n} Non-GIS runs")

    df = enrich_errors_from_detail(df_summary, df_detail)
    df = enrich_geography_from_file_num(df)

    metrics = calculate_metrics(df)

    html_content = generate_html(df, metrics)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"\n✓ Generated {OUTPUT_FILE}")
    print("=" * 60 + "\n")
