'use strict';

const PALETTE = ['#60a5fa', '#f472b6', '#34d399', '#fbbf24', '#a78bfa', '#22d3ee', '#fb923c', '#f87171', '#a3e635', '#e879f9'];
const ROLES = [
    ['local', 'Local site'],
    ['remote', 'Remote site \u00b7 across the bridge'],
    ['internet', 'Internet \u00b7 control'],
];
const PAD = { l: 64, r: 16, t: 12, b: 28 };
const TL = { label: 170, row: 20, gap: 6 };
const TIME_STEPS = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400, 172800, 604800].map((s) => s * 1000);
const FONT = '11px ui-sans-serif, system-ui, "Segoe UI", sans-serif';

const ui = {
    window: Number(localStorage.getItem('bm.window') ?? 900),
    hidden: new Set(JSON.parse(localStorage.getItem('bm.hidden') || '[]')),
    log: localStorage.getItem('bm.log') !== '0',
    data: null,
    series: null,
    hover: -1,
    colors: new Map(),
    timer: 0,
};

const $ = (id) => document.getElementById(id);

function el(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
}

function swatch(color) {
    const s = el('span', 'swatch');
    s.style.background = color;
    return s;
}

// ---------- formatting ----------

function fmtMs(v) {
    if (v == null || !isFinite(v)) return '\u2013';
    if (v < 10) return v.toFixed(2) + ' ms';
    if (v < 100) return v.toFixed(1) + ' ms';
    return Math.round(v).toLocaleString() + ' ms';
}

function fmtPct(v, d = 2) {
    return v == null || !isFinite(v) ? '\u2013' : v.toFixed(d) + '%';
}

function fmtDur(s) {
    if (!s || s < 0) return '0s';
    if (s < 60) return (s < 10 ? s.toFixed(1) : Math.round(s)) + 's';
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ${Math.floor(s % 60)}s`;
    const h = Math.floor(m / 60);
    if (h < 48) return `${h}h ${String(m % 60).padStart(2, '0')}m`;
    return `${Math.floor(h / 24)}d ${h % 24}h`;
}

function fmtClock(ms) {
    return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
}

function fmtDateTime(v) {
    return new Date(v).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
}

function fmtAxisMs(v) {
    const n = Number(v.toPrecision(3));
    return n >= 1000 ? `${n / 1000}s` : `${n}ms`;
}

const colorOf = (name) => ui.colors.get(name) || '#94a3b8';
const lossCls = (v) => (v == null || v === 0 ? '' : v >= 50 ? 'bad-t' : 'warn-t');

// ---------- data loop ----------

function schedule(ms) {
    clearTimeout(ui.timer);
    ui.timer = setTimeout(refresh, ms);
}

async function getJSON(url) {
    const r = await fetch(url, { cache: 'no-store' });
    if (!r.ok) throw new Error(`${url}: ${r.status}`);
    return r.json();
}

function bucketCount() {
    const w = $('latency').clientWidth || 900;
    return Math.max(60, Math.min(1500, Math.floor((w - PAD.l - PAD.r) / 3)));
}

async function refresh() {
    const w = ui.window;
    try {
        const [state, series] = await Promise.all([
            getJSON(`api/state?window=${w}`),
            getJSON(`api/series?window=${w}&buckets=${bucketCount()}`),
        ]);
        if (w !== ui.window) return;
        ui.data = state;
        ui.series = series;
        state.targets.forEach((t, i) => ui.colors.set(t.name, t.color || PALETTE[i % PALETTE.length]));
        $('live').className = 'live-dot on';
        render();
    } catch (err) {
        $('live').className = 'live-dot off';
        $('footer').textContent = `Disconnected: ${err.message}`;
    } finally {
        schedule(1000);
    }
}

function render() {
    renderHeader();
    renderVerdict();
    renderLegend();
    drawLatency();
    drawTimeline();
    renderTargets();
    renderOutages();
}

// ---------- header / verdict ----------

function renderHeader() {
    const d = ui.data;
    $('subtitle').textContent =
        `${d.targets.length} targets \u00b7 every ${d.interval_s}s \u00b7 timeout ${d.timeout_s}s \u00b7 outage = ${d.outage_threshold}+ consecutive losses`;
    const since = d.data_since && !d.data_since.startsWith('0001') ? ` \u00b7 data since ${fmtDateTime(d.data_since)}` : '';
    $('footer').textContent = `Monitor started ${fmtDateTime(d.started)}${since} \u00b7 updated ${fmtClock(Date.parse(d.now))}`;
    $('csv').href = `api/outages.csv?window=${ui.window}`;
}

function kpi(label, value, sub, cls) {
    const c = el('div', 'kpi ' + (cls || ''));
    c.append(el('div', 'kpi-label', label), el('div', 'kpi-value', value));
    if (sub) c.append(el('div', 'kpi-sub', sub));
    return c;
}

const sev = (p) => (p >= 1 ? 'bad' : p > 0 ? 'warn' : 'good');

function renderVerdict() {
    const d = ui.data;
    const v = d.verdict;
    const root = $('verdict');
    root.replaceChildren();

    const head = el('div', 'card-head');
    head.append(el('h2', null, 'Bridge verdict'), el('span', 'muted', `${v.rounds.toLocaleString()} probe rounds over ${fmtDur(d.window_s)}`));
    root.append(head);

    const grid = el('div', 'verdict-grid');
    const inetRemote = d.internet_side === 'remote';
    const inetControl = v.has_internet && !inetRemote;
    const hasFar = v.has_remote || (inetRemote && v.has_internet);
    if (hasFar) {
        const ctrl = inetControl ? 'local + internet OK' : 'local OK';
        grid.append(
            kpi('Bridge-attributable loss', fmtPct(v.bridge_fault_pct), `${v.bridge_fault.toLocaleString()} rounds: far side failed while ${ctrl}`, sev(v.bridge_fault_pct)),
            kpi('Far-side failures caused by bridge', v.far_fail ? fmtPct(v.attribution_pct, 1) : '\u2013', `of ${v.far_fail.toLocaleString()} rounds with far-side loss`),
            kpi('Bridge outages', v.bridge_outages.toLocaleString(), `longest ${fmtDur(v.bridge_longest)} \u00b7 avg ${fmtDur(v.bridge_avg)}`, v.bridge_outages ? 'bad' : 'good'),
            kpi('Bridge downtime', fmtDur(v.bridge_downtime), v.bridge_outages ? `one outage every ${fmtDur(v.bridge_mtbo)}` : 'no outages in window', v.bridge_downtime ? 'bad' : 'good'),
        );
    }
    grid.append(
        kpi('Local site loss (control)', v.has_local ? fmtPct(v.local_fail_pct) : 'n/a', `${v.local_fail.toLocaleString()} rounds`, v.has_local ? sev(v.local_fail_pct) : ''),
        kpi(inetRemote ? 'Internet loss (via bridge)' : 'Internet loss (control)', v.has_internet ? fmtPct(v.internet_fail_pct) : 'n/a', `${v.internet_fail.toLocaleString()} rounds`, v.has_internet ? sev(v.internet_fail_pct) : ''),
    );
    root.append(grid);

    if (hasFar && v.rounds) {
        const ctrl = inetControl ? 'the local site and the internet were' : 'the local site was';
        const text = v.bridge_fault === 0
            ? `No bridge-attributable loss in the last ${fmtDur(d.window_s)}.`
            : `Over the last ${fmtDur(d.window_s)} the far side of the bridge failed to respond in ${v.bridge_fault.toLocaleString()} probe rounds ` +
            `(${fmtPct(v.bridge_fault_pct)}) while ${ctrl} healthy \u2014 ${v.bridge_outages} outage(s) totalling ${fmtDur(v.bridge_downtime)}, ` +
            `longest ${fmtDur(v.bridge_longest)}.`;
        root.append(el('p', 'summary', text));
    }
}

// ---------- legend ----------

function renderLegend() {
    const root = $('legend');
    root.replaceChildren();
    for (const t of ui.data.targets) {
        const b = el('button', 'chip' + (ui.hidden.has(t.name) ? ' off' : ''));
        b.append(swatch(colorOf(t.name)), document.createTextNode(t.name));
        b.addEventListener('click', () => {
            if (ui.hidden.has(t.name)) ui.hidden.delete(t.name);
            else ui.hidden.add(t.name);
            localStorage.setItem('bm.hidden', JSON.stringify([...ui.hidden]));
            renderLegend();
            drawLatency();
        });
        root.append(b);
    }
}

// ---------- canvas helpers ----------

function setupCanvas(cv, h) {
    const dpr = window.devicePixelRatio || 1;
    const w = cv.clientWidth;
    const pw = Math.round(w * dpr);
    const ph = Math.round(h * dpr);
    if (cv.width !== pw || cv.height !== ph) {
        cv.width = pw;
        cv.height = ph;
    }
    const ctx = cv.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    ctx.font = FONT;
    return { ctx, w };
}

function niceCeil(v) {
    if (v <= 0) return 1;
    const p = Math.pow(10, Math.floor(Math.log10(v)));
    const m = v / p;
    return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10) * p;
}

function linTicks(hi, n) {
    const step = niceCeil(hi / n);
    const out = [];
    for (let v = 0; v <= hi + 1e-9; v += step) out.push(v);
    return out;
}

function logTicks(e0, e1) {
    const out = [];
    const dense = e1 - e0 <= 3;
    for (let e = e0; e <= e1; e++) {
        const p = Math.pow(10, e);
        out.push(p);
        if (dense && e < e1) out.push(2 * p, 5 * p);
    }
    return out;
}

function drawTimeAxis(ctx, se, x0, pw, top, bottom) {
    const t0 = se.start;
    const span = se.step * se.buckets;
    const target = span / Math.max(2, pw / 110);
    const step = TIME_STEPS.find((s) => s >= target) || TIME_STEPS[TIME_STEPS.length - 1];
    const off = new Date(t0).getTimezoneOffset() * 60000;
    const withSec = span <= 2 * 3600e3;
    const withDate = span > 2 * 86400e3;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    for (let t = Math.ceil((t0 - off) / step) * step + off; t <= t0 + span; t += step) {
        const x = Math.round(x0 + ((t - t0) / span) * pw) + 0.5;
        ctx.strokeStyle = 'rgba(148,163,184,0.08)';
        ctx.beginPath();
        ctx.moveTo(x, top);
        ctx.lineTo(x, bottom);
        ctx.stroke();
        const d = new Date(t);
        let label = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: withSec ? '2-digit' : undefined, hour12: false });
        if (withDate) label = `${d.getMonth() + 1}/${d.getDate()} ${label}`;
        ctx.fillStyle = '#8b95a7';
        ctx.fillText(label, x, bottom + 8);
    }
}

function drawHover(ctx, se, x0, pw, top, bottom) {
    if (ui.hover < 0 || ui.hover >= se.buckets) return;
    const x = Math.round(x0 + ((ui.hover + 0.5) * pw) / se.buckets) + 0.5;
    ctx.strokeStyle = 'rgba(226,232,240,0.45)';
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(x, top);
    ctx.lineTo(x, bottom);
    ctx.stroke();
    ctx.setLineDash([]);
}

// ---------- latency chart ----------

function drawLatency() {
    const cv = $('latency');
    const h = cv.clientHeight;
    const { ctx, w } = setupCanvas(cv, h);
    const se = ui.series;
    if (!se || !se.buckets) return;

    const pw = w - PAD.l - PAD.r;
    const ph = h - PAD.t - PAD.b;
    const bw = pw / se.buckets;
    const vis = se.targets.filter((t) => !ui.hidden.has(t.name));

    let lo = Infinity;
    let hi = 0;
    for (const t of vis) {
        for (let i = 0; i < se.buckets; i++) {
            if (t.avg[i] != null) lo = Math.min(lo, t.avg[i]);
            if (t.max[i] != null) hi = Math.max(hi, t.max[i]);
        }
    }
    if (!isFinite(lo)) { lo = 1; hi = 10; }

    let Y;
    let ticks;
    if (ui.log) {
        const e0 = Math.floor(Math.log10(Math.max(lo, 0.05)));
        const e1 = Math.max(Math.ceil(Math.log10(Math.max(hi, 1e-3))), e0 + 1);
        const y0 = Math.pow(10, e0);
        Y = (v) => PAD.t + ph * (1 - (Math.log10(Math.max(v, y0)) - e0) / (e1 - e0));
        ticks = logTicks(e0, e1);
    } else {
        const y1 = niceCeil(hi * 1.1);
        Y = (v) => PAD.t + ph * (1 - v / y1);
        ticks = linTicks(y1, 5);
    }
    const X = (i) => PAD.l + (i + 0.5) * bw;

    // loss bands behind everything
    for (let i = 0; i < se.buckets; i++) {
        let l = 0;
        for (const t of vis) if (t.loss[i] > l) l = t.loss[i];
        if (l > 0) {
            ctx.fillStyle = `rgba(239,68,68,${0.12 + 0.38 * Math.min(1, l / 100)})`;
            ctx.fillRect(PAD.l + i * bw, PAD.t, Math.max(bw, 1.5), ph);
        }
    }

    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (const v of ticks) {
        const y = Math.round(Y(v)) + 0.5;
        ctx.strokeStyle = 'rgba(148,163,184,0.10)';
        ctx.beginPath();
        ctx.moveTo(PAD.l, y);
        ctx.lineTo(w - PAD.r, y);
        ctx.stroke();
        ctx.fillStyle = '#8b95a7';
        ctx.fillText(fmtAxisMs(v), PAD.l - 8, y);
    }
    drawTimeAxis(ctx, se, PAD.l, pw, PAD.t, h - PAD.b);

    const path = (arr) => {
        ctx.beginPath();
        let pen = false;
        for (let i = 0; i < arr.length; i++) {
            const v = arr[i];
            if (v == null) { pen = false; continue; }
            const x = X(i);
            const y = Y(v);
            if (pen) ctx.lineTo(x, y);
            else {
                ctx.moveTo(x, y);
                // isolated points would otherwise be invisible
                if (arr[i + 1] == null) ctx.lineTo(x + 0.01, y);
            }
            pen = true;
        }
        ctx.stroke();
    };

    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    for (const t of vis) {
        ctx.strokeStyle = colorOf(t.name);
        ctx.globalAlpha = 0.4;
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        path(t.max);
        ctx.setLineDash([]);
        ctx.globalAlpha = 1;
        ctx.lineWidth = 1.8;
        path(t.avg);
    }

    drawHover(ctx, se, PAD.l, pw, PAD.t, h - PAD.b);
    if (ui.hover >= 0 && ui.hover < se.buckets) {
        for (const t of vis) {
            const v = t.avg[ui.hover];
            if (v == null) continue;
            ctx.fillStyle = colorOf(t.name);
            ctx.beginPath();
            ctx.arc(X(ui.hover), Y(v), 3.5, 0, Math.PI * 2);
            ctx.fill();
        }
    }
}

// ---------- availability timeline ----------

function cellColor(v) {
    if (v == null) return '#1f2735';
    if (v === 0) return '#16a34a';
    const f = Math.min(1, v / 50);
    return `hsl(${Math.round(40 * (1 - f))}, 85%, ${Math.round(52 - 8 * f)}%)`;
}

function timelineRows() {
    const se = ui.series;
    if (!se) return [];
    const rows = se.targets.map((t) => ({ name: t.name, loss: t.loss, color: colorOf(t.name) }));
    if (se.bridge) rows.push({ name: 'Bridge fault', loss: se.bridge, color: '#ef4444' });
    return rows;
}

function drawTimeline() {
    const cv = $('timeline');
    const se = ui.series;
    const rows = timelineRows();
    const h = PAD.t + rows.length * (TL.row + TL.gap) + 22;
    cv.style.height = h + 'px';
    const { ctx, w } = setupCanvas(cv, h);
    if (!se || !se.buckets) return;

    const x0 = TL.label;
    const pw = w - x0 - PAD.r;
    const bw = pw / se.buckets;
    rows.forEach((r, k) => {
        const y = PAD.t + k * (TL.row + TL.gap);
        const bridge = k === se.targets.length;
        if (bridge) {
            ctx.strokeStyle = 'rgba(148,163,184,0.2)';
            ctx.beginPath();
            ctx.moveTo(8, y - TL.gap / 2 + 0.5);
            ctx.lineTo(w - PAD.r, y - TL.gap / 2 + 0.5);
            ctx.stroke();
        }
        ctx.fillStyle = r.color;
        ctx.fillRect(8, y + TL.row / 2 - 4, 8, 8);
        ctx.fillStyle = bridge ? '#f1f5f9' : '#cbd5e1';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        let label = r.name;
        while (label.length > 3 && ctx.measureText(label).width > x0 - 32) label = label.slice(0, -2);
        if (label !== r.name) label += '\u2026';
        ctx.fillText(label, 22, y + TL.row / 2);
        for (let i = 0; i < se.buckets; i++) {
            const xa = Math.floor(x0 + i * bw);
            const xb = Math.floor(x0 + (i + 1) * bw);
            ctx.fillStyle = cellColor(r.loss[i]);
            ctx.fillRect(xa, y, Math.max(1, xb - xa), TL.row);
        }
    });
    drawTimeAxis(ctx, se, x0, pw, PAD.t, h - 22);
    drawHover(ctx, se, x0, pw, PAD.t - 4, h - 22);
}

// ---------- hover / tooltip ----------

function bindHover(cv, x0Of) {
    cv.addEventListener('mousemove', (ev) => {
        const se = ui.series;
        if (!se) return;
        const rect = cv.getBoundingClientRect();
        const x0 = x0Of();
        const pw = rect.width - x0 - PAD.r;
        const i = Math.floor(((ev.clientX - rect.left - x0) / pw) * se.buckets);
        ui.hover = i >= 0 && i < se.buckets ? i : -1;
        drawLatency();
        drawTimeline();
        showTooltip(ev);
    });
    cv.addEventListener('mouseleave', () => {
        ui.hover = -1;
        $('tooltip').hidden = true;
        drawLatency();
        drawTimeline();
    });
}

function showTooltip(ev) {
    const se = ui.series;
    const i = ui.hover;
    const tip = $('tooltip');
    if (!se || i < 0) { tip.hidden = true; return; }

    const t0 = se.start + i * se.step;
    tip.replaceChildren(el('div', 'tip-head', `${fmtClock(t0)} \u2013 ${fmtClock(t0 + se.step)}`));
    const tbl = el('table');
    const hr = el('tr');
    for (const h of ['', 'avg', 'max', 'loss']) hr.append(el('th', null, h));
    tbl.append(hr);
    for (const t of se.targets) {
        if (ui.hidden.has(t.name)) continue;
        const tr = el('tr');
        const name = el('td');
        name.append(swatch(colorOf(t.name)), document.createTextNode(' ' + t.name));
        tr.append(name, el('td', 'num', fmtMs(t.avg[i])), el('td', 'num', fmtMs(t.max[i])), el('td', 'num ' + lossCls(t.loss[i]), fmtPct(t.loss[i], 1)));
        tbl.append(tr);
    }
    if (se.bridge) {
        const tr = el('tr');
        const name = el('td');
        name.append(swatch('#ef4444'), document.createTextNode(' Bridge fault'));
        const td = el('td', 'num ' + lossCls(se.bridge[i]), fmtPct(se.bridge[i], 1));
        td.colSpan = 3;
        tr.append(name, td);
        tbl.append(tr);
    }
    tip.append(tbl);
    tip.hidden = false;

    const pad = 14;
    const r = tip.getBoundingClientRect();
    let x = ev.clientX + pad;
    let y = ev.clientY + pad;
    if (x + r.width > innerWidth) x = ev.clientX - r.width - pad;
    if (y + r.height > innerHeight) y = ev.clientY - r.height - pad;
    tip.style.left = `${Math.max(4, x)}px`;
    tip.style.top = `${Math.max(4, y)}px`;
}

// ---------- target cards ----------

function targetCard(t, d) {
    const w = t.window;
    const recv = w.sent - w.lost;
    const c = el('div', 'tcard' + (t.has_data && !t.up ? ' is-down' : ''));

    const head = el('div', 'tcard-head');
    const title = el('div', 'tcard-title');
    title.append(el('div', 'tname', t.name), el('div', 'thost muted', `${t.type.toUpperCase()} \u00b7 ${t.host}`));
    const status = !t.has_data ? ['idle', 'NO DATA'] : t.up ? ['up', 'UP'] : ['down', 'DOWN'];
    head.append(swatch(colorOf(t.name)), title, el('span', 'pill ' + status[0], status[1]));
    c.append(head);

    const now = el('div', 'tnow');
    if (t.has_data) {
        const forS = (Date.parse(d.now) - Date.parse(t.state_since)) / 1000;
        now.append(
            el('span', 'tnow-rtt' + (t.up ? '' : ' down'), t.up ? fmtMs(t.last_rtt_ms) : t.last_error || 'lost'),
            el('span', 'muted', `${t.up ? 'up' : 'down'} for ${fmtDur(forS)}${t.up ? '' : ` \u00b7 ${t.fail_streak} lost in a row`}`),
        );
    } else {
        now.append(el('span', 'tnow-rtt', '\u2013'));
    }
    c.append(now);

    const g = el('div', 'metrics');
    const m = (label, value, cls) => {
        const x = el('div', 'metric ' + (cls || ''));
        x.append(el('span', 'mlabel', label), el('span', 'mvalue', String(value)));
        g.append(x);
    };
    const rtt = (v) => (recv > 0 ? fmtMs(v) : '\u2013');
    const out = (v) => (w.outages ? fmtDur(v) : '\u2013');
    m('Loss', fmtPct(w.loss_pct), w.loss_pct >= 1 ? 'bad' : w.loss_pct > 0 ? 'warn' : '');
    m('Sent / lost', `${w.sent.toLocaleString()} / ${w.lost.toLocaleString()}`);
    m('Availability', w.sent ? fmtPct(w.availability, 3) : '\u2013', w.availability < 99.9 && w.sent ? 'bad' : '');
    m('Jitter', rtt(w.jitter));
    m('Min', rtt(w.rtt_min));
    m('Avg', rtt(w.rtt_avg));
    m('p95', rtt(w.rtt_p95), w.rtt_p95 > d.spike_ms ? 'warn' : '');
    m('Max', rtt(w.rtt_max), w.rtt_max > d.spike_ms ? 'warn' : '');
    m(`Spikes >${d.spike_ms}ms`, w.spikes.toLocaleString(), w.spikes ? 'warn' : '');
    m('Loss bursts', w.loss_bursts.toLocaleString(), w.loss_bursts ? 'warn' : '');
    m('Max burst', w.max_burst);
    m('Outages', w.outages, w.outages ? 'bad' : '');
    m('Downtime', fmtDur(w.downtime), w.downtime ? 'bad' : '');
    m('Avg outage', out(w.avg_outage));
    m('Longest', out(w.longest_outage));
    m('MTBO', out(w.mtbo));
    c.append(g);
    return c;
}

function renderTargets() {
    const d = ui.data;
    const root = $('targets');
    root.replaceChildren();
    const titles = new Map(ROLES);
    if (d.internet_side === 'remote') titles.set('internet', 'Internet \u00b7 beyond the bridge');
    for (const [role, title] of titles) {
        const list = d.targets.filter((t) => t.role === role);
        if (!list.length) continue;
        const group = el('section', 'role-group');
        const grid = el('div', 'tgrid');
        for (const t of list) grid.append(targetCard(t, d));
        group.append(el('h2', null, title), grid);
        root.append(group);
    }
}

// ---------- outages ----------

function renderOutages() {
    const list = ui.data.outages;
    const tb = $('outages-body');
    tb.replaceChildren();
    $('outage-count').textContent = list.length ? `(${list.length}${list.length >= 200 ? '+' : ''})` : '';
    if (!list.length) {
        const tr = el('tr');
        const td = el('td', 'empty', 'No outages in this window');
        td.colSpan = 6;
        tr.append(td);
        tb.append(tr);
        return;
    }
    for (const o of list.slice(0, 100)) {
        const tr = el('tr');
        const name = el('td');
        name.append(swatch(colorOf(o.target)), document.createTextNode(' ' + o.target));
        const end = o.ongoing ? el('td', null) : el('td', null, fmtDateTime(o.end));
        if (o.ongoing) end.append(el('span', 'pill down', 'ONGOING'));
        tr.append(
            name,
            el('td', 'muted', o.role),
            el('td', null, fmtDateTime(o.start)),
            end,
            el('td', 'num', fmtDur(o.duration_s)),
            el('td', 'num', o.lost.toLocaleString()),
        );
        tb.append(tr);
    }
}

// ---------- init ----------

function init() {
    for (const b of document.querySelectorAll('#windows button')) {
        b.classList.toggle('active', Number(b.dataset.w) === ui.window);
        b.addEventListener('click', () => {
            ui.window = Number(b.dataset.w);
            localStorage.setItem('bm.window', String(ui.window));
            for (const x of document.querySelectorAll('#windows button')) x.classList.toggle('active', x === b);
            schedule(0);
        });
    }
    const log = $('logscale');
    log.checked = ui.log;
    log.addEventListener('change', () => {
        ui.log = log.checked;
        localStorage.setItem('bm.log', ui.log ? '1' : '0');
        drawLatency();
    });
    bindHover($('latency'), () => PAD.l);
    bindHover($('timeline'), () => TL.label);
    new ResizeObserver(() => { drawLatency(); drawTimeline(); }).observe($('latency'));
    refresh();
}

init();
