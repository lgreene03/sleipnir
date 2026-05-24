// Sleipnir Gateway Performance Dashboard — Jsonnet source of truth.
//
// Edit this file, then run:  make dashboard-regen
// Prerequisites:             brew install jsonnet
//
// The compiled output is committed at:
//   telemetry/grafana/provisioning/dashboards/dashboard.json
// Grafana reads the compiled JSON; this file makes diffs reviewable.
//
// Y-coordinate layout:
//   y=0   Core Trading Metrics (row)
//   y=3   Stat trio: Orders Submitted / Fills Dispatched / Gateway Rate
//   y=7   Performance & Latency Profiles (row)
//   y=10  REST Latency (left) | Rate Limiter Delay (right)
//   y=17  Event Streaming Infrastructure (row)
//   y=20  Kafka Broker Throughput (full width)
//   y=27  Operational Health (row)
//   y=28  Active Orders / WS Connection / Daily Orders
//   y=32  Pipeline Latency (row)
//   y=33  Intent-to-Submit (left) | Fill-to-Publish (right)

local promDS = { type: 'prometheus', uid: 'Prometheus' };

// Query target for a Prometheus datasource.
// Pass instant=true for gauge/instant queries; also set range=false for those.
local t(refId, expr, legend, range=true, instant=false) =
  {
    datasource: promDS,
    editorMode: 'code',
    expr: expr,
    legendFormat: legend,
    range: range,
    refId: refId,
  } + if instant then { instant: true } else {};

// Section row (collapsed=false header band).
local row(id, title, y, h=3) = {
  collapsed: false,
  gridPos: { h: h, w: 24, x: 0, y: y },
  id: id,
  title: title,
  type: 'row',
};

// Stat panel with palette-classic colouring — used for the core counter stats.
// startColor is the single threshold step colour (green or purple).
local classicStat(id, title, targets, x, y, w=8, h=4, startColor='green') = {
  datasource: promDS,
  fieldConfig: {
    defaults: {
      color: { mode: 'palette-classic' },
      mappings: [],
      thresholds: {
        mode: 'absolute',
        steps: [{ color: startColor, value: null }],
      },
    },
    overrides: [],
  },
  gridPos: { h: h, w: w, x: x, y: y },
  id: id,
  options: {
    colorMode: 'value',
    graphMode: 'area',
    justifyMode: 'auto',
    orientation: 'auto',
    reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false },
    textMode: 'auto',
  },
  targets: targets,
  title: title,
  type: 'stat',
};

// Timeseries panel.
// Pass unit=null to omit the unit field (e.g. for the Kafka bars panel).
// Pass interp=null to omit lineInterpolation (e.g. when drawStyle='bars').
local tsPanel(id, title, targets, x, y, w=12, h=7, unit='s', draw='line', interp='smooth') = {
  datasource: promDS,
  fieldConfig: {
    defaults: {
      custom:
        { drawStyle: draw }
        + if interp != null then { lineInterpolation: interp } else {},
    } + if unit != null then { unit: unit } else {},
  },
  gridPos: { h: h, w: w, x: x, y: y },
  id: id,
  options: { tooltip: { mode: 'single' } },
  targets: targets,
  title: title,
  type: 'timeseries',
};

// ─── Dashboard ─────────────────────────────────────────────────────────────
{
  annotations: {
    list: [
      {
        builtIn: 1,
        datasource: { type: 'grafana', uid: '-- Grafana --' },
        enable: true,
        hide: true,
        name: 'Annotations & Alerts',
        type: 'dashboard',
      },
    ],
  },
  editable: true,
  fiscalYearStartMonth: 0,
  graphTooltip: 0,
  id: null,
  links: [],
  liveNow: false,

  panels: [

    // ── Core Trading Metrics ───────────────────────────────────────────────
    row(1, 'Core Trading Metrics', y=0),

    classicStat(2, 'Total Orders Submitted',
      targets=[t('A', 'sum(sleipnir_orders_submitted_total)', 'Submitted Orders')],
      x=0, y=3),

    classicStat(3, 'Total Fills Dispatched',
      targets=[t('A', 'sum(sleipnir_orders_filled_total)', 'Filled Orders')],
      x=8, y=3),

    classicStat(4, 'Gateway Rate (Orders/Sec)',
      targets=[t('A', 'sum(rate(sleipnir_orders_submitted_total[1m]))', 'RPS')],
      x=16, y=3, startColor='purple'),

    // ── Performance & Latency Profiles ────────────────────────────────────
    row(5, 'Performance & Latency Profiles', y=7),

    tsPanel(6, 'REST API Operation Latency (p50 & p99)',
      targets=[
        t('A',
          'histogram_quantile(0.99, sum(rate(sleipnir_order_latency_seconds_bucket[1m])) by (le, operation))',
          '{{operation}} (p99)'),
        t('B',
          'histogram_quantile(0.50, sum(rate(sleipnir_order_latency_seconds_bucket[1m])) by (le, operation))',
          '{{operation}} (p50)'),
      ],
      x=0, y=10),

    tsPanel(7, 'Rate Limiter Delay (Sec/Sec)',
      targets=[t('A', 'rate(sleipnir_rate_limit_delay_seconds_total[1m])', 'Delay Rate')],
      x=12, y=10, interp='linear'),

    // ── Event Streaming Infrastructure ────────────────────────────────────
    row(8, 'Event Streaming Infrastructure', y=17),

    // Full-width bars chart — no unit field, no lineInterpolation.
    tsPanel(9, 'Kafka Broker Throughput (Msg/Sec)',
      targets=[t('A',
        'sum(rate(sleipnir_kafka_messages_processed_total[1m])) by (topic, operation, status)',
        '{{topic}} ({{operation}} - {{status}})')],
      x=0, y=20, w=24, unit=null, draw='bars', interp=null),

    // ── Operational Health ────────────────────────────────────────────────
    row(10, 'Operational Health', y=27, h=1),

    // Active orders gauge — green/yellow/red thresholds at 50 and 100.
    {
      datasource: promDS,
      fieldConfig: {
        defaults: {
          color: { mode: 'thresholds' },
          mappings: [],
          thresholds: {
            mode: 'absolute',
            steps: [
              { color: 'green', value: null },
              { color: 'yellow', value: 50 },
              { color: 'red', value: 100 },
            ],
          },
        },
        overrides: [],
      },
      gridPos: { h: 4, w: 8, x: 0, y: 28 },
      id: 11,
      options: {
        colorMode: 'value',
        graphMode: 'none',
        justifyMode: 'auto',
        orientation: 'auto',
        reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false },
        textMode: 'auto',
      },
      targets: [t('A', 'sleipnir_active_orders', 'Active Orders', range=false, instant=true)],
      title: 'Active Orders',
      type: 'stat',
    },

    // WebSocket connection — value mappings give CONNECTED/DISCONNECTED text.
    {
      datasource: promDS,
      fieldConfig: {
        defaults: {
          color: { mode: 'thresholds' },
          mappings: [
            {
              options: {
                '0': { color: 'red', index: 0, text: 'DISCONNECTED' },
                '1': { color: 'green', index: 1, text: 'CONNECTED' },
              },
              type: 'value',
            },
          ],
          thresholds: {
            mode: 'absolute',
            steps: [
              { color: 'red', value: null },
              { color: 'green', value: 1 },
            ],
          },
        },
        overrides: [],
      },
      gridPos: { h: 4, w: 8, x: 8, y: 28 },
      id: 12,
      options: {
        colorMode: 'background',
        graphMode: 'none',
        justifyMode: 'auto',
        orientation: 'auto',
        reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false },
        textMode: 'auto',
      },
      targets: [t('A', 'sleipnir_ws_connected', 'WS Status', range=false, instant=true)],
      title: 'WebSocket Connection',
      type: 'stat',
    },

    // Daily order count — yellow at 400 (83 % of limit), red at 480.
    {
      datasource: promDS,
      fieldConfig: {
        defaults: {
          color: { mode: 'thresholds' },
          mappings: [],
          thresholds: {
            mode: 'absolute',
            steps: [
              { color: 'green', value: null },
              { color: 'yellow', value: 400 },
              { color: 'red', value: 480 },
            ],
          },
          unit: 'short',
        },
        overrides: [],
      },
      gridPos: { h: 4, w: 8, x: 16, y: 28 },
      id: 13,
      options: {
        colorMode: 'value',
        graphMode: 'area',
        justifyMode: 'auto',
        orientation: 'auto',
        reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false },
        textMode: 'auto',
      },
      targets: [t('A', 'sum(increase(sleipnir_orders_submitted_total[24h]))', 'Orders Today')],
      title: 'Daily Orders Consumed (24 h)',
      type: 'stat',
    },

    // ── Pipeline Latency ──────────────────────────────────────────────────
    row(14, 'Pipeline Latency', y=32, h=1),

    tsPanel(15, 'Intent-to-Submit Latency (p50 & p95)',
      targets=[
        t('A',
          'histogram_quantile(0.95, sum(rate(sleipnir_intent_to_submit_seconds_bucket[5m])) by (le))',
          'p95'),
        t('B',
          'histogram_quantile(0.50, sum(rate(sleipnir_intent_to_submit_seconds_bucket[5m])) by (le))',
          'p50'),
      ],
      x=0, y=33),

    tsPanel(16, 'Fill-to-Publish Latency (p50 & p95)',
      targets=[
        t('A',
          'histogram_quantile(0.95, sum(rate(sleipnir_fill_to_publish_seconds_bucket[5m])) by (le))',
          'p95'),
        t('B',
          'histogram_quantile(0.50, sum(rate(sleipnir_fill_to_publish_seconds_bucket[5m])) by (le))',
          'p50'),
      ],
      x=12, y=33),
  ],

  schemaVersion: 38,
  style: 'dark',
  tags: [],
  templating: { list: [] },
  time: { from: 'now-5m', to: 'now' },
  timepicker: {},
  timezone: '',
  title: 'Sleipnir Gateway Performance Dashboard',
  version: 2,
}
