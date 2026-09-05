/* waterplat_dashboard.js
 *
 * Client-side renderer for the Water Plat Tool Usage Dashboard.
 *
 * Reads two globals injected by index.html:
 *   - window.RUNS_DATA       : array of enriched run records (one per Water Plat run)
 *   - window.DASHBOARD_CONFIG: { colors, group_gis, group_non_gis,
 *                                generic_error_stages, stage_colors }
 *
 * Renders every metric card and every chart on load, and re-renders them
 * when the user changes the date range (custom picker or preset chips).
 *
 * Mirrors the Python create_*() functions in waterplat_usage_dashboard.py;
 * the Python file remains the canonical reference for intent, but the JS
 * here is the single source of truth for what users see.
 */
(function () {
    'use strict';

    var RUNS = window.RUNS_DATA || [];
    var CFG = window.DASHBOARD_CONFIG || {};
    var COLORS = CFG.colors || {};
    var STAGE_COLORS = CFG.stage_colors || {};
    var GROUP_GIS = CFG.group_gis;
    var GROUP_NON_GIS = CFG.group_non_gis;
    var CHART_PALETTE = COLORS.chart || ['#e94560', '#4ade80', '#fbbf24', '#38bdf8', '#a78bfa', '#fb923c'];

    // Pre-parse timestamps once
    RUNS.forEach(function (r) {
        r._ts = r.timestamp_start ? new Date(r.timestamp_start) : null;
    });

    var DATA_MIN = null, DATA_MAX = null;
    RUNS.forEach(function (r) {
        if (!r._ts || isNaN(r._ts)) return;
        if (DATA_MIN === null || r._ts < DATA_MIN) DATA_MIN = r._ts;
        if (DATA_MAX === null || r._ts > DATA_MAX) DATA_MAX = r._ts;
    });
    if (!DATA_MIN) DATA_MIN = new Date();
    if (!DATA_MAX) DATA_MAX = new Date();

    // -------------------------------------------------------------------------
    // Generic helpers
    // -------------------------------------------------------------------------

    function pad(n) { return n < 10 ? '0' + n : '' + n; }

    function isoDate(d) {
        return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate());
    }

    function startOfDay(d) {
        var x = new Date(d); x.setHours(0, 0, 0, 0); return x;
    }

    function endOfDay(d) {
        var x = new Date(d); x.setHours(23, 59, 59, 999); return x;
    }

    function weekStartMonday(d) {
        var x = new Date(d);
        x.setHours(0, 0, 0, 0);
        var day = x.getDay();
        var diff = (day === 0 ? -6 : 1 - day);
        x.setDate(x.getDate() + diff);
        return x;
    }

    function dayName(d) {
        return ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][d.getDay()];
    }

    function median(arr) {
        var a = arr.filter(function (v) { return v != null && !isNaN(v); }).slice().sort(function (a, b) { return a - b; });
        if (!a.length) return 0;
        var m = Math.floor(a.length / 2);
        return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
    }

    function quantile(arr, q) {
        var a = arr.filter(function (v) { return v != null && !isNaN(v); }).slice().sort(function (a, b) { return a - b; });
        if (!a.length) return 0;
        var pos = (a.length - 1) * q;
        var base = Math.floor(pos);
        var rest = pos - base;
        return a[base + 1] !== undefined ? a[base] + rest * (a[base + 1] - a[base]) : a[base];
    }

    function uniqueCount(items) {
        var s = new Set();
        items.forEach(function (v) { if (v != null) s.add(v); });
        return s.size;
    }

    function hasErrorMsg(r) {
        var m = r.detail_error_message != null ? r.detail_error_message : r.error_message;
        return m != null && String(m).trim().length > 0;
    }

    function errorMsgOf(r) {
        var m = r.detail_error_message != null ? r.detail_error_message : r.error_message;
        return m == null ? null : String(m);
    }

    // -------------------------------------------------------------------------
    // Filtering
    // -------------------------------------------------------------------------

    function filterByRange(start, end) {
        return RUNS.filter(function (r) {
            return r._ts && r._ts >= start && r._ts <= end;
        });
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    function adoptionRate(runs, col) {
        // % of runs where col > 0 (count-style columns)
        if (!runs.length) return 0;
        var n = 0;
        runs.forEach(function (r) {
            var v = r[col];
            if (v != null && !isNaN(v) && v > 0) n++;
        });
        return n / runs.length * 100;
    }

    function flagTrueRate(runs, col) {
        if (!runs.length) return 0;
        var n = 0;
        runs.forEach(function (r) { if (r[col] === true || r[col] === 1) n++; });
        return n / runs.length * 100;
    }

    function nonNullRate(runs, col) {
        if (!runs.length) return 0;
        var n = 0;
        runs.forEach(function (r) { if (r[col] != null && r[col] !== '') n++; });
        return n / runs.length * 100;
    }

    function computeMetrics(runs) {
        var total = runs.length;
        if (total === 0) {
            return {
                total_runs: 0, unique_users: 0, gis_users: 0, non_gis_users: 0,
                gis_runs: 0, non_gis_runs: 0,
                median_duration: 0, p90_duration: 0,
                non_gis_success_rate: 0, non_gis_error_rate: 0,
                warning_rate: 0, error_types: 0,
            };
        }

        var durations = [];
        var nonGis = [];
        var gisRuns = 0, nonGisRuns = 0;
        var warnings = 0;
        var errorMsgs = new Set();

        runs.forEach(function (r) {
            if (r.duration_seconds != null) durations.push(r.duration_seconds);
            if (r.user_group === GROUP_GIS) gisRuns++;
            if (r.user_group === GROUP_NON_GIS) { nonGisRuns++; nonGis.push(r); }
            if ((r.warning_count || 0) > 0) warnings++;
            if (hasErrorMsg(r)) errorMsgs.add(errorMsgOf(r));
        });

        var nonGisSucc = 0, nonGisErr = 0;
        nonGis.forEach(function (r) {
            if (r.status === 'success') nonGisSucc++;
            if (hasErrorMsg(r)) nonGisErr++;
        });

        return {
            total_runs: total,
            unique_users: uniqueCount(runs.map(function (r) { return r.clean_user; })),
            gis_users: uniqueCount(runs.filter(function (r) { return r.user_group === GROUP_GIS; }).map(function (r) { return r.clean_user; })),
            non_gis_users: uniqueCount(nonGis.map(function (r) { return r.clean_user; })),
            gis_runs: gisRuns,
            non_gis_runs: nonGisRuns,
            median_duration: median(durations),
            p90_duration: quantile(durations, 0.90),
            non_gis_success_rate: nonGisRuns > 0 ? (nonGisSucc / nonGisRuns * 100) : 0,
            non_gis_error_rate: nonGisRuns > 0 ? (nonGisErr / nonGisRuns * 100) : 0,
            warning_rate: total > 0 ? (warnings / total * 100) : 0,
            error_types: errorMsgs.size,
        };
    }

    function formatDuration(seconds) {
        if (seconds == null || isNaN(seconds)) return '—';
        if (seconds >= 60) return (seconds / 60).toFixed(1) + 'm';
        return Math.round(seconds) + 's';
    }

    function formatMetricValue(key, val, format) {
        if (val == null || (typeof val === 'number' && isNaN(val))) return '—';
        if (format === 'duration') return formatDuration(val);
        if (format === 'percent') return val.toFixed(1) + '%';
        if (typeof val === 'number') return Math.round(val).toLocaleString();
        return String(val);
    }

    function renderMetricCards(metrics) {
        document.querySelectorAll('[data-metric]').forEach(function (el) {
            var key = el.dataset.metric;
            var fmt = el.dataset.format;
            el.textContent = formatMetricValue(key, metrics[key], fmt);
        });
    }

    // -------------------------------------------------------------------------
    // Plotly layout helpers
    // -------------------------------------------------------------------------

    function chartLayout(title, height) {
        return {
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            font: { color: COLORS.text, family: 'system-ui, -apple-system, sans-serif', size: 12 },
            margin: { l: 60, r: 30, t: 50, b: 60 },
            title: { text: title, font: { size: 14, color: COLORS.text_muted } },
            xaxis: { gridcolor: 'rgba(255,255,255,0.1)', zerolinecolor: 'rgba(255,255,255,0.1)' },
            yaxis: { gridcolor: 'rgba(255,255,255,0.1)', zerolinecolor: 'rgba(255,255,255,0.1)' },
            height: height || 300,
            autosize: true,
        };
    }

    function emptyAnnotation(text) {
        return {
            text: text, xref: 'paper', yref: 'paper',
            x: 0.5, y: 0.5, showarrow: false,
            font: { color: COLORS.text_muted },
        };
    }

    function plot(id, traces, layout) {
        var el = document.getElementById(id);
        if (!el) return;
        Plotly.react(el, traces, layout, { responsive: true, displaylogo: false });
    }

    function renderEmpty(id, title, message, height) {
        var layout = chartLayout(title, height || 300);
        layout.annotations = [emptyAnnotation(message || 'No data in selected range')];
        plot(id, [], layout);
    }

    // -------------------------------------------------------------------------
    // Aggregation helpers
    // -------------------------------------------------------------------------

    function valueCounts(arr) {
        var m = new Map();
        arr.forEach(function (v) {
            if (v == null) return;
            m.set(v, (m.get(v) || 0) + 1);
        });
        return Array.from(m.entries())
            .map(function (e) { return { key: e[0], count: e[1] }; })
            .sort(function (a, b) { return b.count - a.count; });
    }

    function rollingMean(values, window) {
        var n = values.length;
        var out = new Array(n);
        var half = Math.floor(window / 2);
        for (var i = 0; i < n; i++) {
            var lo = Math.max(0, i - half);
            var hi = Math.min(n - 1, i + (window - 1 - half));
            var sum = 0, cnt = 0;
            for (var j = lo; j <= hi; j++) {
                if (values[j] != null && !isNaN(values[j])) { sum += values[j]; cnt++; }
            }
            out[i] = cnt > 0 ? sum / cnt : null;
        }
        return out;
    }

    // -------------------------------------------------------------------------
    // Chart renderers
    // -------------------------------------------------------------------------

    function renderWeeklyTrend(id, runs) {
        if (!runs.length) { renderEmpty(id, 'Weekly Run Trend'); return; }

        var weekly = new Map();
        runs.forEach(function (r) {
            var w = weekStartMonday(r._ts).getTime();
            var k = w + '|' + r.user_group;
            weekly.set(k, (weekly.get(k) || 0) + 1);
        });

        var currentWeek = weekStartMonday(new Date()).getTime();
        var groups = [
            { name: GROUP_GIS, color: CHART_PALETTE[3] },
            { name: GROUP_NON_GIS, color: CHART_PALETTE[5] },
        ];

        var traces = [];
        groups.forEach(function (g) {
            var pts = [];
            weekly.forEach(function (count, k) {
                var parts = k.split('|');
                if (parts[1] !== g.name) return;
                pts.push({ week: parseInt(parts[0], 10), runs: count });
            });
            pts.sort(function (a, b) { return a.week - b.week; });

            var completed = pts.filter(function (p) { return p.week < currentWeek; });
            var partial = pts.filter(function (p) { return p.week === currentWeek; });

            if (completed.length) {
                traces.push({
                    type: 'scatter', mode: 'lines+markers',
                    x: completed.map(function (p) { return new Date(p.week); }),
                    y: completed.map(function (p) { return p.runs; }),
                    name: g.name,
                    line: { color: g.color },
                    marker: { color: g.color },
                });
            }

            if (partial.length && completed.length) {
                traces.push({
                    type: 'scatter', mode: 'lines+markers',
                    x: [new Date(completed[completed.length - 1].week), new Date(partial[0].week)],
                    y: [completed[completed.length - 1].runs, partial[0].runs],
                    showlegend: false,
                    line: { color: g.color, dash: 'dash' },
                    marker: { color: g.color, symbol: 'circle-open', size: 8 },
                });
            } else if (partial.length) {
                traces.push({
                    type: 'scatter', mode: 'markers',
                    x: [new Date(partial[0].week)],
                    y: [partial[0].runs],
                    showlegend: false,
                    name: g.name,
                    marker: { color: g.color, symbol: 'circle-open', size: 8 },
                });
            }
        });

        var layout = chartLayout('Weekly Run Trend');
        layout.margin = { l: 60, r: 30, t: 80, b: 60 };
        layout.yaxis.title = { text: 'Total Runs' };
        layout.legend = { orientation: 'h', yanchor: 'bottom', y: 1.02, xanchor: 'center', x: 0.5 };
        layout.annotations = [{
            text: 'Dashed = current week (partial)',
            xref: 'paper', yref: 'paper', x: 1.0, y: -0.25,
            showarrow: false, xanchor: 'right',
            font: { size: 10, color: COLORS.text_muted },
        }];
        plot(id, traces, layout);
    }

    function statusBucket(status) {
        var s = String(status || '').toLowerCase();
        if (s.indexOf('success') >= 0 || s.indexOf('warning') >= 0) return 'success';
        return 'error';
    }

    function renderUserDistribution(id, runs, group, title) {
        var grpRuns = runs.filter(function (r) { return r.user_group === group; });
        if (!grpRuns.length) { renderEmpty(id, title, 'No ' + group + ' runs', 320); return; }

        var counts = valueCounts(grpRuns.map(function (r) { return r.clean_user; })).slice(0, 10);
        var topUsers = counts.map(function (c) { return c.key; });
        var topSet = new Set(topUsers);

        var bucketByUser = { success: {}, error: {} };
        topUsers.forEach(function (u) {
            bucketByUser.success[u] = 0;
            bucketByUser.error[u] = 0;
        });
        grpRuns.forEach(function (r) {
            if (!topSet.has(r.clean_user)) return;
            bucketByUser[statusBucket(r.status)][r.clean_user]++;
        });

        var palette = { success: COLORS.success, error: COLORS.error };
        var traces = ['error', 'success'].map(function (b) {
            return {
                type: 'bar', orientation: 'h',
                name: b,
                x: topUsers.map(function (u) { return bucketByUser[b][u]; }),
                y: topUsers.slice(),
                marker: { color: palette[b] },
            };
        });

        var layout = chartLayout(title, 320);
        layout.barmode = 'stack';
        layout.margin = { l: 160, r: 30, t: 80, b: 60 };
        layout.yaxis.categoryorder = 'total ascending';
        layout.yaxis.automargin = true;
        layout.xaxis.title = { text: 'Number of runs' };
        layout.legend = { orientation: 'h', yanchor: 'bottom', y: 1.02, xanchor: 'center', x: 0.5 };
        plot(id, traces, layout);
    }

    function renderUserDistGis(id, runs) {
        renderUserDistribution(id, runs, GROUP_GIS, 'Top 10 GIS Users');
    }

    function renderUserDistNonGis(id, runs) {
        renderUserDistribution(id, runs, GROUP_NON_GIS, 'Top 10 Non-GIS Users');
    }

    function renderRegionDistribution(id, runs) {
        var withRegion = runs.filter(function (r) { return r.region != null; });
        if (!withRegion.length) { renderEmpty(id, 'Runs by Region'); return; }

        var byRegionGroup = new Map();
        withRegion.forEach(function (r) {
            var bucket = byRegionGroup.get(r.region);
            if (!bucket) { bucket = {}; bucket[GROUP_GIS] = 0; bucket[GROUP_NON_GIS] = 0; byRegionGroup.set(r.region, bucket); }
            bucket[r.user_group] = (bucket[r.user_group] || 0) + 1;
        });

        var regions = Array.from(byRegionGroup.keys());
        regions.sort(function (a, b) {
            var ta = byRegionGroup.get(a)[GROUP_GIS] + byRegionGroup.get(a)[GROUP_NON_GIS];
            var tb = byRegionGroup.get(b)[GROUP_GIS] + byRegionGroup.get(b)[GROUP_NON_GIS];
            return ta - tb;
        });

        var palette = {};
        palette[GROUP_GIS] = CHART_PALETTE[3];
        palette[GROUP_NON_GIS] = CHART_PALETTE[5];

        var traces = [GROUP_GIS, GROUP_NON_GIS].map(function (g) {
            return {
                type: 'bar', orientation: 'h',
                name: g,
                x: regions.map(function (r) { return byRegionGroup.get(r)[g] || 0; }),
                y: regions.slice(),
                marker: { color: palette[g] },
            };
        });

        var layout = chartLayout('Runs by Region');
        layout.margin = { l: 140, r: 30, t: 80, b: 60 };
        layout.yaxis.automargin = true;
        layout.barmode = 'stack';
        layout.xaxis.title = { text: 'Number of runs' };
        layout.legend = { orientation: 'h', yanchor: 'bottom', y: 1.02, xanchor: 'center', x: 0.5 };
        plot(id, traces, layout);
    }

    function renderUserGroupSplit(id, runs) {
        if (!runs.length) { renderEmpty(id, 'Runs by User Group', 'No data', 320); return; }

        var counts = new Map();
        runs.forEach(function (r) {
            counts.set(r.user_group, (counts.get(r.user_group) || 0) + 1);
        });
        var labels = Array.from(counts.keys());
        var values = labels.map(function (l) { return counts.get(l); });
        var palette = {};
        palette[GROUP_GIS] = CHART_PALETTE[3];
        palette[GROUP_NON_GIS] = CHART_PALETTE[5];
        var colors = labels.map(function (l) { return palette[l] || COLORS.text_muted; });

        var traces = [{
            type: 'pie', labels: labels, values: values, hole: 0.4,
            marker: { colors: colors },
            textposition: 'inside', textinfo: 'percent+label',
        }];
        var layout = chartLayout('Runs by User Group', 320);
        plot(id, traces, layout);
    }

    function renderUsageHeatmap(id, runs) {
        var dayOrder = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'];
        var hours = [];
        for (var h = 6; h <= 18; h++) hours.push(h);

        var grid = {};
        dayOrder.forEach(function (d) {
            grid[d] = {};
            hours.forEach(function (h) { grid[d][h] = 0; });
        });

        runs.forEach(function (r) {
            if (!r._ts) return;
            var d = dayName(r._ts);
            if (dayOrder.indexOf(d) < 0) return;
            var hr = r._ts.getHours();
            if (hr < 6 || hr > 18) return;
            grid[d][hr]++;
        });

        var z = dayOrder.map(function (d) { return hours.map(function (h) { return grid[d][h]; }); });

        var traces = [{
            type: 'heatmap',
            z: z,
            x: hours.map(function (h) { return h + ':00'; }),
            y: dayOrder.slice(),
            colorscale: [[0, COLORS.bg_secondary], [0.5, COLORS.warning], [1, COLORS.accent]],
            hovertemplate: '%{y} at %{x}<br>Runs: %{z}<extra></extra>',
        }];
        var layout = chartLayout('Peak Usage (Day × Hour)', 320);
        layout.xaxis.title = { text: 'Hour of Day' };
        layout.yaxis.autorange = 'reversed';
        plot(id, traces, layout);
    }

    function renderStatusDistribution(id, runs) {
        var ng = runs.filter(function (r) { return r.user_group === GROUP_NON_GIS; });
        if (!ng.length) { renderEmpty(id, 'Status Distribution (Non-GIS)', 'No Non-GIS runs', 320); return; }

        var counts = valueCounts(ng.map(function (r) { return r.status; }));
        var labels = counts.map(function (c) { return c.key; });
        var values = counts.map(function (c) { return c.count; });
        var colors = labels.map(function (l) { return l === 'success' ? COLORS.success : COLORS.error; });

        var traces = [{
            type: 'pie', labels: labels, values: values, hole: 0.4,
            marker: { colors: colors },
            textposition: 'inside', textinfo: 'percent+label',
        }];
        plot(id, traces, chartLayout('Status Distribution (Non-GIS)', 320));
    }

    function renderFailureRateTrend(id, runs) {
        var ng = runs.filter(function (r) { return r.user_group === GROUP_NON_GIS; });
        if (!ng.length) { renderEmpty(id, 'Weekly Failure Rate Trend (Non-GIS)', 'No data available', 350); return; }

        var weekly = new Map();
        ng.forEach(function (r) {
            var w = weekStartMonday(r._ts).getTime();
            var b = weekly.get(w);
            if (!b) { b = { total: 0, errors: 0 }; weekly.set(w, b); }
            b.total++;
            if (hasErrorMsg(r)) b.errors++;
        });
        var weeks = Array.from(weekly.keys()).sort(function (a, b) { return a - b; });
        if (!weeks.length) { renderEmpty(id, 'Weekly Failure Rate Trend (Non-GIS)', 'No data available', 350); return; }

        var weeklyTotal = 0, weeklyErrors = 0;
        var failureRate = weeks.map(function (w) {
            var b = weekly.get(w);
            weeklyTotal += b.total; weeklyErrors += b.errors;
            return b.total > 0 ? b.errors / b.total * 100 : 0;
        });
        if (weeklyTotal === 0) { renderEmpty(id, 'Weekly Failure Rate Trend (Non-GIS)', 'No data available', 350); return; }

        var overallAvg = weeklyErrors / weeklyTotal * 100;
        var ma = rollingMean(failureRate, 3);

        // Per-region weekly aggregation — drop rows missing region
        var regionWeekly = new Map();
        var regionErrCounts = new Map();
        ng.forEach(function (r) {
            if (r.region == null) return;
            var w = weekStartMonday(r._ts).getTime();
            var rm = regionWeekly.get(r.region);
            if (!rm) { rm = new Map(); regionWeekly.set(r.region, rm); }
            var b = rm.get(w);
            if (!b) { b = { total: 0, errors: 0 }; rm.set(w, b); }
            b.total++;
            if (hasErrorMsg(r)) {
                b.errors++;
                regionErrCounts.set(r.region, (regionErrCounts.get(r.region) || 0) + 1);
            }
        });

        var regionOrder = Array.from(regionErrCounts.entries())
            .sort(function (a, b) { return b[1] - a[1]; })
            .map(function (e) { return e[0]; });
        regionWeekly.forEach(function (_, region) {
            if (regionOrder.indexOf(region) < 0) regionOrder.push(region);
        });
        var regionColor = {};
        regionOrder.forEach(function (region, i) {
            regionColor[region] = CHART_PALETTE[i % CHART_PALETTE.length];
        });

        var traces = [];

        regionOrder.forEach(function (region) {
            var rm = regionWeekly.get(region);
            var pts = weeks.map(function (w) {
                var b = rm.get(w);
                return b && b.total > 0 ? (b.errors / b.total * 100) : null;
            });
            var smoothed = rollingMean(pts, 3);
            var xs = [], ys = [];
            weeks.forEach(function (w, i) {
                if (smoothed[i] != null) { xs.push(new Date(w)); ys.push(smoothed[i]); }
            });
            if (!xs.length) return;
            traces.push({
                type: 'scatter', mode: 'lines',
                name: region,
                x: xs, y: ys,
                line: { color: regionColor[region], width: 1.7 },
                opacity: 0.5,
                hovertemplate: '<b>' + region + '</b><br>Week: %{x|%b %d}<br>Failure rate: %{y:.1f}%<extra></extra>',
            });
        });

        traces.push({
            type: 'scatter', mode: 'lines+markers',
            name: 'Overall',
            x: weeks.map(function (w) { return new Date(w); }),
            y: failureRate,
            line: { color: COLORS.text, width: 3 },
            marker: { color: COLORS.text, size: 6 },
            hovertemplate: '<b>Overall</b><br>Week: %{x|%b %d}<br>Failure rate: %{y:.1f}%<extra></extra>',
        });

        traces.push({
            type: 'scatter', mode: 'lines',
            name: '3-wk trend',
            x: weeks.map(function (w) { return new Date(w); }),
            y: ma,
            line: { color: COLORS.accent, width: 2.5, dash: 'dash' },
            hovertemplate: '<b>3-week moving avg</b><br>Week: %{x|%b %d}<br>Trend: %{y:.1f}%<extra></extra>',
        });

        var layout = chartLayout('Weekly Failure Rate Trend (Non-GIS)', 350);
        layout.margin = { l: 60, r: 30, t: 80, b: 60 };
        layout.yaxis.title = { text: 'Failure Rate (%)' };
        layout.yaxis.rangemode = 'tozero';
        layout.legend = { orientation: 'h', yanchor: 'bottom', y: 1.02, xanchor: 'center', x: 0.5 };
        layout.shapes = [{
            type: 'rect', xref: 'paper', yref: 'y',
            x0: 0, x1: 1,
            y0: Math.max(overallAvg - 2, 0), y1: overallAvg + 2,
            fillcolor: COLORS.text_muted, opacity: 0.08,
            line: { width: 0 },
            layer: 'below',
        }, {
            type: 'line', xref: 'paper', yref: 'y',
            x0: 0, x1: 1, y0: overallAvg, y1: overallAvg,
            line: { color: COLORS.text_muted, width: 1, dash: 'dot' },
            layer: 'below',
        }];
        layout.annotations = [{
            text: 'Avg ' + overallAvg.toFixed(1) + '%',
            xref: 'paper', yref: 'y',
            x: 0, y: overallAvg, xanchor: 'left', yanchor: 'bottom',
            showarrow: false,
            font: { size: 11, color: COLORS.text_muted },
        }];
        plot(id, traces, layout);
    }

    function renderErrorMessages(id, runs) {
        var ng = runs.filter(function (r) { return r.user_group === GROUP_NON_GIS && hasErrorMsg(r); });
        if (!ng.length) { renderEmpty(id, 'Common Error Messages - Non-GIS (from Detail Logs)', 'No errors recorded', 380); return; }

        var byMsg = new Map();
        ng.forEach(function (r) {
            var msg = errorMsgOf(r);
            if (!msg) return;
            var b = byMsg.get(msg);
            if (!b) { b = { count: 0, stages: new Map() }; byMsg.set(msg, b); }
            b.count++;
            if (r.detail_error_stage != null) {
                b.stages.set(r.detail_error_stage, (b.stages.get(r.detail_error_stage) || 0) + 1);
            }
        });

        var rows = Array.from(byMsg.entries())
            .map(function (e) {
                var modeStage = 'unknown', modeCount = 0;
                e[1].stages.forEach(function (cnt, st) {
                    if (cnt > modeCount) { modeStage = st; modeCount = cnt; }
                });
                return { message: e[0], count: e[1].count, stage: modeStage };
            })
            .sort(function (a, b) { return b.count - a.count; })
            .slice(0, 8);

        var traces = rows.map(function (row) {
            var color = STAGE_COLORS[row.stage] || COLORS.error;
            return {
                type: 'bar', orientation: 'h',
                y: [row.message], x: [row.count],
                marker: { color: color },
                name: row.stage,
                showlegend: false,
                hovertemplate: '<b>' + row.message + '</b><br>Stage: ' + row.stage + '<br>Count: ' + row.count + '<extra></extra>',
                text: [row.count + '  [' + row.stage + ']'],
                textposition: 'outside',
                textfont: { size: 11 },
            };
        });

        var layout = chartLayout('Common Error Messages - Non-GIS (from Detail Logs)', 380);
        layout.yaxis.categoryorder = 'total ascending';
        layout.yaxis.automargin = true;
        layout.margin = { l: 380, r: 100, t: 50, b: 60 };
        plot(id, traces, layout);
    }

    function renderErrorByRegion(id, runs) {
        var ng = runs.filter(function (r) { return r.user_group === GROUP_NON_GIS && hasErrorMsg(r); });
        if (!ng.length) { renderEmpty(id, 'Errors by Region (Non-GIS)', 'No errors recorded', 320); return; }

        var withRegion = ng.filter(function (r) { return r.region != null; });
        if (!withRegion.length) { renderEmpty(id, 'Errors by Region (Non-GIS)', 'No errors with region', 320); return; }

        var counts = valueCounts(withRegion.map(function (r) { return r.region; }));
        var labels = counts.map(function (c) { return c.key; });
        var values = counts.map(function (c) { return c.count; });
        var colors = labels.map(function (_, i) { return CHART_PALETTE[i % CHART_PALETTE.length]; });

        var traces = [{
            type: 'pie', labels: labels, values: values, hole: 0.4,
            marker: { colors: colors },
            textposition: 'inside', textinfo: 'percent+label',
        }];
        plot(id, traces, chartLayout('Errors by Region (Non-GIS)', 320));
    }

    function renderErrorStages(id, runs) {
        var ng = runs.filter(function (r) { return r.user_group === GROUP_NON_GIS && hasErrorMsg(r); });
        if (!ng.length) { renderEmpty(id, 'Top Error Stages (Non-GIS)', 'No errors recorded', 320); return; }

        var stages = ng.map(function (r) { return r.detail_error_stage; })
                      .filter(function (s) { return s != null; });

        if (!stages.length) { renderEmpty(id, 'Top Error Stages (Non-GIS)', 'No errors recorded', 320); return; }

        var counts = valueCounts(stages);
        var labels = counts.map(function (c) { return c.key; });
        var values = counts.map(function (c) { return c.count; });
        var colors = labels.map(function (l) { return STAGE_COLORS[l] || COLORS.text_muted; });

        var traces = [{
            type: 'pie', labels: labels, values: values, hole: 0.4,
            marker: { colors: colors },
            textposition: 'inside', textinfo: 'percent+label',
        }];
        plot(id, traces, chartLayout('Top Error Stages (Non-GIS)', 320));
    }

    function renderMapTypeDistribution(id, runs) {
        var withType = runs.filter(function (r) { return r.map_type != null; });
        if (!withType.length) { renderEmpty(id, 'Map Type Distribution', 'No data', 320); return; }

        var counts = valueCounts(withType.map(function (r) { return r.map_type; }));
        var total = counts.reduce(function (s, c) { return s + c.count; }, 0);
        var threshold = 0.02 * total;

        var main = [];
        var otherTotal = 0;
        counts.forEach(function (c) {
            if (c.count >= threshold) main.push(c);
            else otherTotal += c.count;
        });
        if (otherTotal > 0) main.push({ key: 'Other', count: otherTotal });

        var labels = main.map(function (c) { return c.key; });
        var values = main.map(function (c) { return c.count; });
        var colors = main.map(function (_, i) { return CHART_PALETTE[i % CHART_PALETTE.length]; });

        var traces = [{
            type: 'pie', labels: labels, values: values, hole: 0.4,
            marker: { colors: colors },
            textposition: 'inside', textinfo: 'percent+label',
        }];
        plot(id, traces, chartLayout('Map Type Distribution', 320));
    }

    function renderLayerAdoption(id, runs) {
        var total = runs.length;
        if (total === 0) { renderEmpty(id, 'ID Type Usage (% of runs)', 'No data', 350); return; }

        var layers = [
            { label: 'PODs', col: 'pod_count' },
            { label: 'PIDs', col: 'pid_count' },
            { label: 'Well Tags', col: 'well_tag_count' },
            { label: 'Mineral Tenures', col: 'mineral_count' },
            { label: 'Range Tenures', col: 'range_count' },
            { label: 'Special Use Permits', col: 'su_count' },
        ];

        var rates = layers.map(function (l) {
            return { feature: l.label, rate: adoptionRate(runs, l.col) };
        });

        var traces = [{
            type: 'bar', orientation: 'h',
            x: rates.map(function (r) { return r.rate; }),
            y: rates.map(function (r) { return r.feature; }),
            marker: { color: CHART_PALETTE[3] },
            text: rates.map(function (r) { return r.rate.toFixed(1) + '%'; }),
            textposition: 'outside',
        }];
        var layout = chartLayout('ID Type Usage (% of runs)', 350);
        layout.xaxis.title = { text: 'Usage %' };
        layout.xaxis.range = [0, 100];
        layout.margin = { l: 160, r: 60, t: 50, b: 60 };
        plot(id, traces, layout);
    }

    function renderOptionalFlags(id, runs) {
        var total = runs.length;
        if (total === 0) { renderEmpty(id, 'Optional Features Usage', 'No data', 320); return; }

        var flags = [
            { label: 'UOT', col: 'uot', mode: 'true' },
            { label: 'Temp PD provided', col: 'temp_pd_provided', mode: 'true' },
            { label: 'Temp Storage provided', col: 'temp_st_provided', mode: 'true' },
            { label: 'Scale override', col: 'scale_override', mode: 'non_null' },
        ];

        var rates = flags.map(function (f) {
            var rate = f.mode === 'true' ? flagTrueRate(runs, f.col) : nonNullRate(runs, f.col);
            return { feature: f.label, rate: rate };
        });

        var traces = [{
            type: 'bar', orientation: 'h',
            x: rates.map(function (r) { return r.rate; }),
            y: rates.map(function (r) { return r.feature; }),
            marker: { color: CHART_PALETTE[4] },
            text: rates.map(function (r) { return r.rate.toFixed(1) + '%'; }),
            textposition: 'outside',
        }];
        var layout = chartLayout('Optional Features Usage', 320);
        layout.xaxis.title = { text: 'Usage %' };
        layout.xaxis.range = [0, 100];
        layout.margin = { l: 180, r: 60, t: 50, b: 60 };
        plot(id, traces, layout);
    }

    // -------------------------------------------------------------------------
    // Apply filter — recompute everything for the given window
    // -------------------------------------------------------------------------

    function applyFilter(start, end) {
        var filtered = filterByRange(start, end);
        var metrics = computeMetrics(filtered);
        renderMetricCards(metrics);

        var countEl = document.getElementById('filter-count');
        if (countEl) countEl.textContent = filtered.length + ' of ' + RUNS.length + ' runs';

        renderWeeklyTrend('chart-weekly_trend', filtered);
        renderRegionDistribution('chart-region_dist', filtered);
        renderUserGroupSplit('chart-user_group_split', filtered);
        renderUserDistGis('chart-user_dist_gis', filtered);
        renderUserDistNonGis('chart-user_dist_non_gis', filtered);
        renderUsageHeatmap('chart-usage_heatmap', filtered);
        renderFailureRateTrend('chart-failure_rate_trend', filtered);
        renderStatusDistribution('chart-status_dist', filtered);
        renderErrorByRegion('chart-error_region', filtered);
        renderErrorStages('chart-error_stages', filtered);
        renderErrorMessages('chart-error_msgs', filtered);
        renderMapTypeDistribution('chart-map_type_dist', filtered);
        renderLayerAdoption('chart-layer_adoption', filtered);
        renderOptionalFlags('chart-optional_flags', filtered);
    }

    // -------------------------------------------------------------------------
    // Picker wiring
    // -------------------------------------------------------------------------

    function setActivePreset(name) {
        document.querySelectorAll('.preset-btn').forEach(function (btn) {
            btn.classList.toggle('active', btn.dataset.preset === name);
        });
    }

    function presetRange(name) {
        var end = endOfDay(DATA_MAX);
        var start;
        switch (name) {
            case '7d':  start = new Date(end); start.setDate(start.getDate() - 7); break;
            case '30d': start = new Date(end); start.setDate(start.getDate() - 30); break;
            case '90d': start = new Date(end); start.setDate(start.getDate() - 90); break;
            case '6mo': start = new Date(end); start.setMonth(start.getMonth() - 6); break;
            case 'ytd': start = new Date(end.getFullYear(), 0, 1); start.setHours(0, 0, 0, 0); break;
            case 'all':
            default: start = startOfDay(DATA_MIN); break;
        }
        if (start < startOfDay(DATA_MIN)) start = startOfDay(DATA_MIN);
        return { start: start, end: end };
    }

    function init() {
        var startInput = document.getElementById('filter-start');
        var endInput = document.getElementById('filter-end');
        var applyBtn = document.getElementById('filter-apply');
        var resetBtn = document.getElementById('filter-reset');
        var errEl = document.getElementById('filter-error');

        var minIso = isoDate(DATA_MIN);
        var maxIso = isoDate(DATA_MAX);

        if (startInput) {
            startInput.min = minIso; startInput.max = maxIso;
            startInput.value = minIso;
        }
        if (endInput) {
            endInput.min = minIso; endInput.max = maxIso;
            endInput.value = maxIso;
        }

        function clearError() { if (errEl) errEl.textContent = ''; }
        function showError(msg) { if (errEl) errEl.textContent = msg; }

        function readPicker() {
            var sParts = (startInput && startInput.value || minIso).split('-').map(Number);
            var eParts = (endInput && endInput.value || maxIso).split('-').map(Number);
            var s = new Date(sParts[0], sParts[1] - 1, sParts[2], 0, 0, 0, 0);
            var e = new Date(eParts[0], eParts[1] - 1, eParts[2], 23, 59, 59, 999);
            return { start: s, end: e };
        }

        if (applyBtn) {
            applyBtn.addEventListener('click', function () {
                var r = readPicker();
                if (r.end < r.start) { showError('End date must be on or after start date'); return; }
                clearError();
                setActivePreset(null);
                applyFilter(r.start, r.end);
            });
        }
        if (resetBtn) {
            resetBtn.addEventListener('click', function () {
                if (startInput) startInput.value = minIso;
                if (endInput) endInput.value = maxIso;
                clearError();
                setActivePreset('all');
                applyFilter(startOfDay(DATA_MIN), endOfDay(DATA_MAX));
            });
        }

        document.querySelectorAll('.preset-btn').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var name = btn.dataset.preset;
                var r = presetRange(name);
                if (startInput) startInput.value = isoDate(r.start);
                if (endInput) endInput.value = isoDate(r.end);
                clearError();
                setActivePreset(name);
                applyFilter(r.start, r.end);
            });
        });

        setActivePreset('all');
        applyFilter(startOfDay(DATA_MIN), endOfDay(DATA_MAX));

        window.addEventListener('resize', function () {
            document.querySelectorAll('.js-plotly-plot').forEach(function (p) { Plotly.Plots.resize(p); });
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
