# Plotly Integration Plan: Data Availability Timeline

## Goal
Create a "data availability histogram" showing which data types exist for which time periods. This serves as both a useful diagnostic tool and a proof-of-concept for Plotly integration.

## Current State
- **Frontend**: Pure CSS, no JavaScript libraries, no charting
- **Backend**: Flask + Jinja2, InfluxDB queries via `influx_reader.py`
- **Charting**: Plotly.js for all graphs

## InfluxDB Measurements Available

| Measurement | Data Type | Typical Frequency |
|-------------|-----------|-------------------|
| `heating_system` | Heating | Every 15 min |
| `weather_observation` | Weather | Every 15 min |
| `weather_forecast` | Weather | Every 2 hours |
| `heating_control` | ML Control | On decisions |
| `thermal_learning` | ML Learning | On updates |
| `heat_curve_adjustment` | Active Control | On changes |
| `temperature_forecast` | ML Predictions | Every 2 hours |
| `thermal_history` | Historical | Every 15 min |

## Visualization Design

### Data Availability Timeline (Gantt-style)

```
Category          | Jan 20    Jan 21    Jan 22    Jan 23    Jan 24
------------------|--------------------------------------------------
Heating Data      | ████████████████████████████████████████████████
Weather Obs       | ████████████████████████████████████████████████
Weather Forecast  | ████████████████████████████████████████████████
ML Predictions    |           ████████████████████████████████████████
Active Control    |                     ██████        ████████
Energy Import     |                                              (future)
```

Each row shows data density/coverage for that category:
- **Full bar**: Continuous data
- **Gaps**: Missing periods
- **Color intensity**: Could indicate data quality/density

### Interactivity
- **Pan/Zoom**: Navigate time range
- **Hover**: Show exact counts per time bucket
- **Click**: Jump to detailed view for that period

## Implementation Plan

### Phase 1: Backend (influx_reader.py)

Add method to query data availability:

```python
def get_data_availability(self, house_id: str, start: str = "-30d", end: str = "now()") -> dict:
    """
    Query each measurement for data point counts per time bucket.
    Returns dict with measurement -> list of (timestamp, count) tuples.
    """
    measurements = [
        ("heating_system", "Heating Data"),
        ("weather_observation", "Weather"),
        ("weather_forecast", "Forecasts"),
        ("heating_control", "ML Control"),
        ("temperature_forecast", "ML Predictions"),
        ("heat_curve_adjustment", "Active Control"),
    ]
    # Query each with aggregateWindow(every: 1h, fn: count)
```

### Phase 2: API Endpoint (app.py)

```python
@app.route('/api/house/<house_id>/data-availability')
@login_required
def api_data_availability(house_id):
    """Return data availability for Plotly chart."""
    data = influx.get_data_availability(house_id, start="-30d")
    return jsonify(data)
```

### Phase 3: Frontend Integration

**Option A: Server-side Plotly (simpler)**
- Generate Plotly JSON in Python
- Render with `plotly.js` CDN in template
- No build step needed

**Option B: Client-side fetch (more interactive)**
- Fetch data via API
- Build chart in JavaScript with Plotly.js
- Better for dynamic updates

**Recommended: Option A first**, then migrate to B if needed.

### Phase 4: Template & Styling

New template: `templates/house_graphs.html`

```html
{% extends "base.html" %}
{% block content %}
<div class="card">
    <h2>Data Availability</h2>
    <div id="availability-chart" style="height: 400px;"></div>
</div>

<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<script>
    const graphData = {{ graph_json | safe }};
    Plotly.newPlot('availability-chart', graphData.data, graphData.layout, {
        responsive: true,
        displayModeBar: true,
        modeBarButtonsToRemove: ['lasso2d', 'select2d']
    });
</script>
{% endblock %}
```

## File Changes

### New Files
| File | Purpose |
|------|---------|
| `static/js/charts.js` | Shared Plotly utilities (optional, later) |

### Modified Files
| File | Changes |
|------|---------|
| `influx_reader.py` | Add `get_data_availability()` method |
| `app.py` | Add `/api/house/<id>/data-availability` route |
| `app.py` | Add `/house/<id>/graphs` route |
| `templates/house_detail.html` | Add link to graphs page |
| `templates/house_graphs.html` | New template with Plotly chart |

## Dependencies

```bash
# Add to requirements.txt
plotly>=5.18.0
```

No npm/webpack needed - using Plotly.js from CDN.

## Chart Type for Availability

Use **Plotly timeline/heatmap hybrid**:

```python
import plotly.graph_objects as go

fig = go.Figure()

# Each measurement as a horizontal bar
for measurement, display_name, data in measurements:
    fig.add_trace(go.Bar(
        y=[display_name] * len(data),
        x=[bucket['count'] for bucket in data],  # Width = count
        base=[bucket['time'] for bucket in data],  # Position = time
        orientation='h',
        name=display_name,
        hovertemplate='%{base}: %{x} points<extra></extra>'
    ))

fig.update_layout(
    barmode='overlay',
    xaxis_type='date',
    height=400,
    margin=dict(l=150)
)
```

Alternative: **Heatmap** with time on X, measurement on Y, color = density.

## Milestones

1. **M1**: `get_data_availability()` working in Python REPL
2. **M2**: API endpoint returning JSON
3. **M3**: Static Plotly chart rendering in template
4. **M4**: Pan/zoom working
5. **M5**: Link from house_detail.html

## Future Extensions

Once this works, we can add:
- **Temperature history graph** (line chart with pan/zoom)
- **Forecast accuracy graph** (predicted vs actual)
- **Energy planning graph** (interactive point setting)
- **Scatter: Supply temp vs Outdoor temp** (heat curve visualization)

## Design Decisions

### Time Range
- **Default**: 1 month (30 days)
- **Selection**: Both UI controls AND pan/zoom in graph
- **Sync**: Zooming updates the date picker, picking dates updates the graph

### Visual Language for Data Types

| Data Type | Line Style | Example |
|-----------|------------|---------|
| **Measured** | Solid line | Room temp, supply temp, weather obs |
| **Predicted** | Dotted line | ML forecasts, temperature predictions |

```
Measured:    ────────────────────
Predicted:   ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄
```

**Exceptions** (future): Deferred until concept is clearer.

### Color Palette (Proposed)

Use consistent colors across all graphs:

| Data Category | Color | Hex |
|---------------|-------|-----|
| Indoor/Room temp | Warm orange | #e67e22 |
| Outdoor temp | Cool blue | #3498db |
| Supply temp | Red | #e74c3c |
| Return temp | Pink | #ff7979 |
| Weather | Sky blue | #74b9ff |
| ML/Predictions | Purple (dotted) | #9b59b6 |
| Control actions | Green | #27ae60 |

## Implementation Ready

Core scope defined:
- **Measured data**: Solid lines (heating_system, weather_observation)
- **Predicted data**: Dotted lines (temperature_forecast, weather_forecast)
- **Time range**: 30 days default, pan/zoom + UI controls
- **Exceptions**: Deferred to future iteration
