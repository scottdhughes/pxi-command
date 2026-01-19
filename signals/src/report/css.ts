// ─────────────────────────────────────────────────────────────────────────────
// PXI-Signals Report CSS
// Design: Matches pxicommand.com - dark terminal aesthetic
// ─────────────────────────────────────────────────────────────────────────────

export const REPORT_CSS = `
/* ═══════════════════════════════════════════════════════════════════════════
   CSS VARIABLES - Design Tokens (matching pxicommand.com)
   ═══════════════════════════════════════════════════════════════════════════ */
:root {
  /* Base palette - matching pxicommand.com */
  --bg: #000000;
  --bg-elevated: #0a0a0a;
  --panel: #0a0a0a;
  --panel-hover: #111111;
  --text: #f3f3f3;
  --text-secondary: #949ba5;
  --muted: #949ba5;
  --border: #26272b;
  --border-glow: #26272b;

  /* Brand accent - matching pxicommand.com blue */
  --accent: #00a3ff;
  --accent-glow: rgba(0, 163, 255, 0.3);
  --accent-dark: #0066aa;
  --accent-subtle: rgba(0, 163, 255, 0.1);

  /* Semantic colors */
  --success: #10b981;
  --success-subtle: rgba(16, 185, 129, 0.12);
  --warning: #f59e0b;
  --warning-subtle: rgba(245, 158, 11, 0.12);
  --danger: #ff6b6b;
  --danger-subtle: rgba(255, 107, 107, 0.12);
  --info: #00a3ff;
  --info-subtle: rgba(0, 163, 255, 0.12);

  /* Timing colors */
  --timing-now: #00a3ff;
  --timing-now-bg: rgba(0, 163, 255, 0.15);
  --timing-building: #6366f1;
  --timing-building-bg: rgba(99, 102, 241, 0.15);
  --timing-early: #8b5cf6;
  --timing-early-bg: rgba(139, 92, 246, 0.15);
  --timing-ongoing: #f59e0b;
  --timing-ongoing-bg: rgba(245, 158, 11, 0.15);

  /* Signal type colors */
  --signal-rotation: #00a3ff;
  --signal-momentum: #6366f1;
  --signal-divergence: #f59e0b;
  --signal-reversion: #8b5cf6;

  /* Confidence colors */
  --conf-very-high: #00a3ff;
  --conf-high: #00a3ff;
  --conf-medium-high: #22d3d8;
  --conf-medium: #f59e0b;
  --conf-low: #ff6b6b;

  /* Typography - system mono like pxicommand.com */
  --font-display: ui-monospace, 'SF Mono', Menlo, Monaco, 'Cascadia Mono', monospace;
  --font-mono: ui-monospace, 'SF Mono', Menlo, Monaco, 'Cascadia Mono', monospace;

  /* Spacing */
  --space-xs: 4px;
  --space-sm: 8px;
  --space-md: 16px;
  --space-lg: 24px;
  --space-xl: 32px;
  --space-2xl: 48px;

  /* Borders */
  --radius-sm: 4px;
  --radius-md: 8px;
  --radius-lg: 12px;
  --radius-pill: 999px;
}

/* ═══════════════════════════════════════════════════════════════════════════
   RESET & BASE
   ═══════════════════════════════════════════════════════════════════════════ */
*, *::before, *::after {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  font-family: var(--font-mono);
  font-size: 14px;
  line-height: 1.6;
  background: var(--bg);
  color: var(--text);
  -webkit-font-smoothing: antialiased;
  min-height: 100vh;
}

/* Grid texture overlay */
body::before {
  content: '';
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background-image:
    linear-gradient(rgba(0, 179, 134, 0.02) 1px, transparent 1px),
    linear-gradient(90deg, rgba(0, 179, 134, 0.02) 1px, transparent 1px);
  background-size: 40px 40px;
  pointer-events: none;
  z-index: 0;
}

/* ═══════════════════════════════════════════════════════════════════════════
   SITE NAVIGATION - Matching pxicommand.com main page
   ═══════════════════════════════════════════════════════════════════════════ */
.site-nav {
  position: sticky;
  top: 0;
  z-index: 100;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--space-md) var(--space-lg);
  background: var(--bg);
}

.site-nav-brand {
  display: flex;
  align-items: center;
  text-decoration: none;
  font-family: var(--font-display);
  font-size: 13px;
  font-weight: 400;
  letter-spacing: 0.08em;
  color: rgba(255, 255, 255, 0.6);
  text-transform: uppercase;
  transition: color 0.15s;
  padding: 8px 0;
}

.site-nav-brand:hover {
  color: rgba(255, 255, 255, 0.85);
}

.brand-slash {
  margin: 0;
}

.brand-caret {
  margin-left: 6px;
  font-size: 8px;
  opacity: 0.7;
}

.site-nav-date {
  font-family: var(--font-mono);
  font-size: 13px;
  letter-spacing: 0.05em;
  color: rgba(255, 255, 255, 0.5);
}

.site-nav-links {
  display: flex;
  align-items: center;
  gap: var(--space-md);
}

.nav-link {
  font-family: var(--font-mono);
  font-size: 11px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--accent);
  text-decoration: none;
  padding: 8px 16px;
  border: 1px solid var(--accent);
  border-radius: 0;
  transition: all 0.15s;
}

.nav-link:hover {
  background: var(--accent);
  color: var(--bg);
}

/* ═══════════════════════════════════════════════════════════════════════════
   LAYOUT
   ═══════════════════════════════════════════════════════════════════════════ */
.container {
  position: relative;
  z-index: 1;
  max-width: 1080px;
  margin: 0 auto;
  padding: var(--space-xl) var(--space-lg) 80px;
}

/* ═══════════════════════════════════════════════════════════════════════════
   HERO HEADER
   ═══════════════════════════════════════════════════════════════════════════ */
.hero {
  text-align: center;
  padding: var(--space-2xl) 0;
  margin-bottom: var(--space-xl);
  border-bottom: 1px solid var(--border);
  position: relative;
}

.hero::after {
  content: '';
  position: absolute;
  bottom: -1px;
  left: 50%;
  transform: translateX(-50%);
  width: 200px;
  height: 2px;
  background: linear-gradient(90deg, transparent, var(--accent), transparent);
}

.hero-title {
  font-family: var(--font-display);
  font-size: 32px;
  font-weight: 700;
  letter-spacing: -0.02em;
  color: var(--text);
  margin-bottom: var(--space-md);
  text-transform: uppercase;
}

.hero-title .accent {
  color: var(--accent);
  text-shadow: 0 0 20px var(--accent-glow);
}

.hero-meta {
  display: flex;
  justify-content: center;
  gap: var(--space-xl);
  flex-wrap: wrap;
}

.hero-meta-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--space-xs);
}

.hero-meta-label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.15em;
  color: var(--muted);
}

.hero-meta-value {
  font-size: 14px;
  font-weight: 500;
  color: var(--text-secondary);
}

/* ═══════════════════════════════════════════════════════════════════════════
   SUMMARY DASHBOARD
   ═══════════════════════════════════════════════════════════════════════════ */
.summary-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: var(--space-md);
  margin-bottom: var(--space-xl);
}

.stat-card {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: var(--space-lg);
  text-align: center;
  position: relative;
  overflow: hidden;
  transition: border-color 0.2s, transform 0.2s;
}

.stat-card:hover {
  border-color: var(--border-glow);
  transform: translateY(-2px);
}

.stat-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 2px;
  background: var(--accent);
  opacity: 0.6;
}

.stat-card.highlight::before {
  background: linear-gradient(90deg, var(--accent), var(--timing-building));
}

.stat-label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: var(--muted);
  margin-bottom: var(--space-sm);
}

.stat-value {
  font-family: var(--font-display);
  font-size: 28px;
  font-weight: 600;
  color: var(--text);
  line-height: 1.2;
}

.stat-value.accent {
  color: var(--accent);
  text-shadow: 0 0 15px var(--accent-glow);
}

.stat-sub {
  font-size: 11px;
  color: var(--muted);
  margin-top: var(--space-xs);
}

/* ═══════════════════════════════════════════════════════════════════════════
   SIGNAL DISTRIBUTION BAR
   ═══════════════════════════════════════════════════════════════════════════ */
.distribution-section {
  margin-bottom: var(--space-xl);
}

.distribution-bar {
  display: flex;
  height: 32px;
  border-radius: var(--radius-md);
  overflow: hidden;
  background: var(--panel);
  border: 1px solid var(--border);
}

.dist-segment {
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 11px;
  font-weight: 500;
  color: var(--bg);
  transition: flex-grow 0.3s ease;
  min-width: 0;
  overflow: hidden;
  white-space: nowrap;
}

.dist-segment.rotation { background: var(--signal-rotation); }
.dist-segment.momentum { background: var(--signal-momentum); }
.dist-segment.divergence { background: var(--signal-divergence); }
.dist-segment.reversion { background: var(--signal-reversion); }

.dist-legend {
  display: flex;
  justify-content: center;
  gap: var(--space-lg);
  margin-top: var(--space-md);
  flex-wrap: wrap;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: var(--space-sm);
  font-size: 12px;
  color: var(--text-secondary);
}

.legend-dot {
  width: 10px;
  height: 10px;
  border-radius: 2px;
}

.legend-dot.rotation { background: var(--signal-rotation); }
.legend-dot.momentum { background: var(--signal-momentum); }
.legend-dot.divergence { background: var(--signal-divergence); }
.legend-dot.reversion { background: var(--signal-reversion); }

/* ═══════════════════════════════════════════════════════════════════════════
   SECTION HEADERS
   ═══════════════════════════════════════════════════════════════════════════ */
.section {
  margin-bottom: var(--space-xl);
}

.section-header {
  display: flex;
  align-items: center;
  gap: var(--space-md);
  margin-bottom: var(--space-lg);
}

.section-title {
  font-family: var(--font-display);
  font-size: 14px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--muted);
}

.section-line {
  flex: 1;
  height: 1px;
  background: linear-gradient(90deg, var(--border), transparent);
}

/* ═══════════════════════════════════════════════════════════════════════════
   THEME CARDS - Major Component
   ═══════════════════════════════════════════════════════════════════════════ */
.theme-card {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  margin-bottom: var(--space-lg);
  overflow: hidden;
  transition: border-color 0.2s;
}

.theme-card:hover {
  border-color: var(--border-glow);
}

.theme-card-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  padding: var(--space-lg);
  border-bottom: 1px solid var(--border);
  gap: var(--space-md);
}

.theme-rank-badge {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 2px;
  min-width: 48px;
}

.rank-number {
  font-family: var(--font-display);
  font-size: 24px;
  font-weight: 700;
  color: var(--accent);
  line-height: 1;
}

.rank-stars {
  font-size: 10px;
  letter-spacing: 1px;
  color: var(--warning);
}

.rank-stars .empty {
  color: var(--muted);
  opacity: 0.4;
}

.theme-info {
  flex: 1;
}

.theme-name {
  font-family: var(--font-display);
  font-size: 18px;
  font-weight: 600;
  color: var(--text);
  margin-bottom: var(--space-sm);
}

.theme-pills {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-sm);
}

.pill {
  display: inline-flex;
  align-items: center;
  padding: 4px 10px;
  border-radius: var(--radius-pill);
  font-size: 11px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

/* Signal type pills */
.pill-rotation {
  background: rgba(0, 179, 134, 0.15);
  color: var(--signal-rotation);
  border: 1px solid rgba(0, 179, 134, 0.3);
}
.pill-momentum {
  background: rgba(59, 130, 246, 0.15);
  color: var(--signal-momentum);
  border: 1px solid rgba(59, 130, 246, 0.3);
}
.pill-divergence {
  background: rgba(245, 158, 11, 0.15);
  color: var(--signal-divergence);
  border: 1px solid rgba(245, 158, 11, 0.3);
}
.pill-reversion {
  background: rgba(139, 92, 246, 0.15);
  color: var(--signal-reversion);
  border: 1px solid rgba(139, 92, 246, 0.3);
}

/* Confidence pills */
.pill-conf-very-high {
  background: rgba(16, 185, 129, 0.15);
  color: var(--conf-very-high);
  border: 1px solid rgba(16, 185, 129, 0.3);
}
.pill-conf-high {
  background: rgba(0, 179, 134, 0.15);
  color: var(--conf-high);
  border: 1px solid rgba(0, 179, 134, 0.3);
}
.pill-conf-medium-high {
  background: rgba(34, 211, 216, 0.15);
  color: var(--conf-medium-high);
  border: 1px solid rgba(34, 211, 216, 0.3);
}
.pill-conf-medium {
  background: rgba(245, 158, 11, 0.15);
  color: var(--conf-medium);
  border: 1px solid rgba(245, 158, 11, 0.3);
}
.pill-conf-low {
  background: rgba(239, 68, 68, 0.15);
  color: var(--conf-low);
  border: 1px solid rgba(239, 68, 68, 0.3);
}

/* Timing pills */
.pill-timing-now {
  background: var(--timing-now-bg);
  color: var(--timing-now);
  border: 1px solid rgba(0, 179, 134, 0.4);
}
.pill-timing-building {
  background: var(--timing-building-bg);
  color: var(--timing-building);
  border: 1px solid rgba(99, 102, 241, 0.4);
}
.pill-timing-early {
  background: var(--timing-early-bg);
  color: var(--timing-early);
  border: 1px solid rgba(139, 92, 246, 0.4);
}
.pill-timing-ongoing {
  background: var(--timing-ongoing-bg);
  color: var(--timing-ongoing);
  border: 1px solid rgba(245, 158, 11, 0.4);
}

/* Sparkline in header */
.theme-sparkline {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
}

.sparkline-label {
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--muted);
}

.sparkline {
  font-family: var(--font-mono);
  font-size: 16px;
  letter-spacing: 2px;
  color: var(--accent);
  text-shadow: 0 0 8px var(--accent-glow);
  line-height: 1;
}

.sparkline.negative {
  color: var(--danger);
  text-shadow: 0 0 8px rgba(239, 68, 68, 0.3);
}

/* Theme card body */
.theme-card-body {
  padding: var(--space-lg);
}

/* Metrics grid */
.metrics-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: var(--space-md);
  margin-bottom: var(--space-lg);
}

.metric-item {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: var(--space-md);
  text-align: center;
}

.metric-label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--muted);
  margin-bottom: var(--space-xs);
}

.metric-value {
  font-family: var(--font-display);
  font-size: 20px;
  font-weight: 600;
  color: var(--text);
}

.metric-value.positive {
  color: var(--success);
}

.metric-value.negative {
  color: var(--danger);
}

.metric-value.neutral {
  color: var(--text-secondary);
}

/* Ticker tags */
.tickers-row {
  margin-bottom: var(--space-lg);
}

.tickers-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--muted);
  margin-bottom: var(--space-sm);
}

.ticker-tags {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-sm);
}

.ticker-tag {
  display: inline-flex;
  align-items: center;
  padding: 6px 12px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  font-family: var(--font-mono);
  font-size: 12px;
  font-weight: 600;
  color: var(--accent);
  transition: all 0.15s;
}

.ticker-tag:hover {
  background: var(--accent-subtle);
  border-color: var(--accent);
  transform: translateY(-1px);
}

/* Narrative text */
.narrative {
  font-size: 13px;
  line-height: 1.7;
  color: var(--text-secondary);
  margin-bottom: var(--space-lg);
  padding: var(--space-md);
  background: var(--bg);
  border-left: 2px solid var(--border);
  border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
}

/* Caution banner */
.caution-banner {
  display: flex;
  align-items: center;
  gap: var(--space-sm);
  padding: var(--space-md);
  background: var(--danger-subtle);
  border: 1px solid rgba(239, 68, 68, 0.3);
  border-radius: var(--radius-md);
  margin-bottom: var(--space-lg);
}

.caution-icon {
  font-size: 16px;
}

.caution-text {
  font-size: 12px;
  color: var(--danger);
  font-weight: 500;
}

/* Evidence section - collapsible */
.evidence-section {
  border-top: 1px solid var(--border);
  margin-top: var(--space-md);
}

.evidence-toggle {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding: var(--space-md) 0;
  background: none;
  border: none;
  color: var(--muted);
  font-family: var(--font-mono);
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  cursor: pointer;
  transition: color 0.15s;
}

.evidence-toggle:hover {
  color: var(--text-secondary);
}

.evidence-toggle-icon {
  transition: transform 0.2s;
}

.evidence-section.open .evidence-toggle-icon {
  transform: rotate(180deg);
}

.evidence-list {
  display: none;
  padding-bottom: var(--space-md);
}

.evidence-section.open .evidence-list {
  display: block;
}

.evidence-item {
  padding: var(--space-sm) 0;
  border-bottom: 1px solid var(--border);
}

.evidence-item:last-child {
  border-bottom: none;
}

.evidence-link {
  display: block;
  color: var(--accent);
  text-decoration: none;
  font-size: 12px;
  word-break: break-all;
  transition: color 0.15s;
}

.evidence-link:hover {
  color: var(--text);
  text-decoration: underline;
}

/* ═══════════════════════════════════════════════════════════════════════════
   KEY TAKEAWAYS
   ═══════════════════════════════════════════════════════════════════════════ */
.takeaways-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: var(--space-md);
}

.takeaway-card {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: var(--space-lg);
  position: relative;
  overflow: hidden;
}

.takeaway-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 3px;
}

.takeaway-card.data::before {
  background: var(--success);
}

.takeaway-card.actionable::before {
  background: var(--info);
}

.takeaway-card.risks::before {
  background: var(--danger);
}

.takeaway-title {
  font-family: var(--font-display);
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  margin-bottom: var(--space-md);
}

.takeaway-card.data .takeaway-title {
  color: var(--success);
}

.takeaway-card.actionable .takeaway-title {
  color: var(--info);
}

.takeaway-card.risks .takeaway-title {
  color: var(--danger);
}

.takeaway-list {
  list-style: none;
}

.takeaway-list li {
  position: relative;
  padding-left: var(--space-md);
  margin-bottom: var(--space-sm);
  font-size: 13px;
  color: var(--text-secondary);
  line-height: 1.5;
}

.takeaway-list li::before {
  content: '›';
  position: absolute;
  left: 0;
  color: var(--muted);
}

/* ═══════════════════════════════════════════════════════════════════════════
   FOOTER
   ═══════════════════════════════════════════════════════════════════════════ */
.footer {
  text-align: center;
  padding: var(--space-xl) 0;
  margin-top: var(--space-2xl);
  border-top: 1px solid var(--border);
}

.footer-text {
  font-size: 11px;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.1em;
}

.footer-disclaimer {
  font-size: 12px;
  color: var(--muted);
  margin-top: var(--space-sm);
  opacity: 0.7;
}

/* ═══════════════════════════════════════════════════════════════════════════
   ANIMATIONS
   ═══════════════════════════════════════════════════════════════════════════ */
@keyframes fadeInUp {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.6;
  }
}

.theme-card {
  animation: fadeInUp 0.4s ease forwards;
  opacity: 0;
}

.theme-card:nth-child(1) { animation-delay: 0.05s; }
.theme-card:nth-child(2) { animation-delay: 0.1s; }
.theme-card:nth-child(3) { animation-delay: 0.15s; }
.theme-card:nth-child(4) { animation-delay: 0.2s; }
.theme-card:nth-child(5) { animation-delay: 0.25s; }
.theme-card:nth-child(6) { animation-delay: 0.3s; }
.theme-card:nth-child(7) { animation-delay: 0.35s; }
.theme-card:nth-child(8) { animation-delay: 0.4s; }
.theme-card:nth-child(9) { animation-delay: 0.45s; }
.theme-card:nth-child(10) { animation-delay: 0.5s; }

.stat-card {
  animation: fadeInUp 0.3s ease forwards;
  opacity: 0;
}

.stat-card:nth-child(1) { animation-delay: 0.1s; }
.stat-card:nth-child(2) { animation-delay: 0.15s; }
.stat-card:nth-child(3) { animation-delay: 0.2s; }
.stat-card:nth-child(4) { animation-delay: 0.25s; }

/* ═══════════════════════════════════════════════════════════════════════════
   RESPONSIVE - Tablet (768px)
   ═══════════════════════════════════════════════════════════════════════════ */
@media (max-width: 768px) {
  .container {
    padding: var(--space-lg) var(--space-md) 60px;
  }

  .hero-title {
    font-size: 24px;
  }

  .summary-grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .metrics-grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .takeaways-grid {
    grid-template-columns: 1fr;
  }

  .theme-card-header {
    flex-wrap: wrap;
  }

  .theme-sparkline {
    width: 100%;
    align-items: flex-start;
    margin-top: var(--space-md);
    padding-top: var(--space-md);
    border-top: 1px solid var(--border);
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   RESPONSIVE - Mobile (480px)
   ═══════════════════════════════════════════════════════════════════════════ */
@media (max-width: 480px) {
  .hero-title {
    font-size: 20px;
  }

  .hero-meta {
    flex-direction: column;
    gap: var(--space-md);
  }

  .summary-grid {
    grid-template-columns: 1fr;
  }

  .stat-value {
    font-size: 24px;
  }

  .metrics-grid {
    grid-template-columns: 1fr;
  }

  .theme-name {
    font-size: 16px;
  }

  .dist-legend {
    flex-direction: column;
    align-items: flex-start;
    gap: var(--space-sm);
  }

  .theme-pills {
    gap: var(--space-xs);
  }

  .pill {
    font-size: 10px;
    padding: 3px 8px;
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   UTILITY CLASSES
   ═══════════════════════════════════════════════════════════════════════════ */
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  border: 0;
}

.text-accent { color: var(--accent); }
.text-success { color: var(--success); }
.text-warning { color: var(--warning); }
.text-danger { color: var(--danger); }
.text-muted { color: var(--muted); }

/* ═══════════════════════════════════════════════════════════════════════════
   ACCURACY TRACK RECORD - Matching pxicommand.com minimal style
   ═══════════════════════════════════════════════════════════════════════════ */
.accuracy-card {
  margin-bottom: var(--space-xl);
  animation: fadeInUp 0.4s ease forwards;
  animation-delay: 0.3s;
  opacity: 0;
}

.accuracy-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--space-lg);
  padding-bottom: var(--space-md);
  border-bottom: 1px dashed rgba(255, 255, 255, 0.1);
}

.accuracy-title {
  font-family: var(--font-display);
  font-size: 11px;
  font-weight: 400;
  text-transform: uppercase;
  letter-spacing: 0.15em;
  color: rgba(255, 255, 255, 0.4);
}

.accuracy-sample {
  display: flex;
  align-items: center;
  gap: var(--space-xs);
  font-size: 11px;
  color: rgba(255, 255, 255, 0.4);
}

.accuracy-sample-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--accent);
  animation: pulse 2s infinite;
}

.accuracy-body {
  display: grid;
  grid-template-columns: 200px 1fr;
  gap: var(--space-xl);
  align-items: start;
}

.accuracy-main {
  text-align: center;
}

.accuracy-hero-value {
  font-family: var(--font-display);
  font-size: 72px;
  font-weight: 300;
  color: var(--text);
  line-height: 1;
  letter-spacing: -0.02em;
}

.accuracy-hero-value .unit {
  font-size: 32px;
  color: rgba(255, 255, 255, 0.4);
  font-weight: 300;
}

.accuracy-hero-label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.2em;
  color: rgba(255, 255, 255, 0.35);
  margin-top: var(--space-sm);
}

.accuracy-hero-sub {
  margin-top: var(--space-md);
  font-size: 13px;
  color: rgba(255, 255, 255, 0.5);
}

.accuracy-hero-sub .value {
  color: var(--text);
  font-weight: 500;
}

.accuracy-hero-sub .value.positive {
  color: var(--success);
}

.accuracy-hero-sub .value.negative {
  color: var(--danger);
}

.accuracy-breakdown {
  display: flex;
  flex-direction: column;
  gap: var(--space-md);
}

.accuracy-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--space-sm) 0;
}

.accuracy-row-label {
  font-size: 12px;
  text-transform: lowercase;
  color: rgba(255, 255, 255, 0.5);
  letter-spacing: 0.02em;
}

.accuracy-row-bar {
  flex: 1;
  height: 4px;
  background: rgba(255, 255, 255, 0.08);
  margin: 0 var(--space-lg);
  position: relative;
  overflow: hidden;
}

.accuracy-row-fill {
  position: absolute;
  top: 0;
  left: 0;
  height: 100%;
  transition: width 0.6s ease;
}

.accuracy-row-fill.now {
  background: var(--accent);
}

.accuracy-row-fill.building {
  background: var(--timing-building);
}

.accuracy-row-fill.early {
  background: var(--timing-early);
}

.accuracy-row-values {
  display: flex;
  align-items: baseline;
  gap: var(--space-md);
  min-width: 120px;
  justify-content: flex-end;
}

.accuracy-row-rate {
  font-family: var(--font-display);
  font-size: 18px;
  font-weight: 400;
  color: var(--text);
}

.accuracy-row-count {
  font-size: 11px;
  color: rgba(255, 255, 255, 0.35);
}

/* No data state */
.accuracy-no-data {
  padding: var(--space-2xl) var(--space-xl);
  text-align: center;
  border: 1px dashed rgba(255, 255, 255, 0.1);
}

.accuracy-no-data-text {
  font-size: 13px;
  color: rgba(255, 255, 255, 0.5);
  margin-bottom: var(--space-xs);
}

.accuracy-no-data-sub {
  font-size: 11px;
  color: rgba(255, 255, 255, 0.3);
}

/* Responsive for accuracy card */
@media (max-width: 768px) {
  .accuracy-body {
    grid-template-columns: 1fr;
    gap: var(--space-lg);
  }

  .accuracy-hero-value {
    font-size: 56px;
  }

  .accuracy-row-bar {
    display: none;
  }
}

@media (max-width: 480px) {
  .accuracy-header {
    flex-direction: column;
    gap: var(--space-sm);
    text-align: center;
  }

  .accuracy-hero-value {
    font-size: 48px;
  }
}
`
