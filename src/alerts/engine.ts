import axios from 'axios';
import { format } from 'date-fns';
import { query } from '../db/connection.js';

interface Alert {
  type: string;
  message: string;
  severity: 'info' | 'warning' | 'critical';
  score?: number;
  delta?: number;
}

// ============== Alert Detection ==============

export async function detectAlerts(date: Date): Promise<Alert[]> {
  const alerts: Alert[] = [];
  const dateStr = format(date, 'yyyy-MM-dd');

  // Get current and historical PXI scores
  const result = await query(`
    SELECT date, score, label, status
    FROM pxi_scores
    ORDER BY date DESC
    LIMIT 10
  `);

  if (result.rows.length === 0) return alerts;

  const current = result.rows[0];
  const currentScore = parseFloat(current.score);

  // 1. Zone crossing alerts
  if (result.rows.length >= 2) {
    const previous = result.rows[1];
    const prevScore = parseFloat(previous.score);

    // Crossed into PAMP zone
    if (currentScore >= 65 && prevScore < 65) {
      alerts.push({
        type: 'zone_cross_pamp',
        message: `PXI entered PAMP zone: ${currentScore.toFixed(1)}`,
        severity: 'info',
        score: currentScore,
      });
    }

    // Crossed into DUMP zone
    if (currentScore < 35 && prevScore >= 35) {
      alerts.push({
        type: 'zone_cross_dump',
        message: `PXI entered DUMP zone: ${currentScore.toFixed(1)}`,
        severity: 'critical',
        score: currentScore,
      });
    }

    // Rapid movement (>5 points in a day)
    const dailyMove = currentScore - prevScore;
    if (Math.abs(dailyMove) >= 5) {
      alerts.push({
        type: 'rapid_move',
        message: `PXI moved ${dailyMove > 0 ? '+' : ''}${dailyMove.toFixed(1)} in 1 day`,
        severity: dailyMove < 0 ? 'warning' : 'info',
        delta: dailyMove,
      });
    }
  }

  // 2. Extreme readings
  if (currentScore >= 85) {
    alerts.push({
      type: 'extreme_high',
      message: `PXI at extreme high: ${currentScore.toFixed(1)} - consider caution`,
      severity: 'warning',
      score: currentScore,
    });
  }

  if (currentScore <= 20) {
    alerts.push({
      type: 'extreme_low',
      message: `PXI at extreme low: ${currentScore.toFixed(1)} - potential opportunity`,
      severity: 'warning',
      score: currentScore,
    });
  }

  // 3. Category divergence
  const catResult = await query(
    `SELECT category, score FROM category_scores WHERE date = $1`,
    [dateStr]
  );

  if (catResult.rows.length >= 4) {
    const scores = catResult.rows.map((r) => parseFloat(r.score));
    const maxScore = Math.max(...scores);
    const minScore = Math.min(...scores);

    if (maxScore - minScore > 40) {
      const highCat = catResult.rows.find((r) => parseFloat(r.score) === maxScore)?.category;
      const lowCat = catResult.rows.find((r) => parseFloat(r.score) === minScore)?.category;

      alerts.push({
        type: 'category_divergence',
        message: `Large divergence: ${highCat} (${maxScore.toFixed(0)}) vs ${lowCat} (${minScore.toFixed(0)})`,
        severity: 'info',
      });
    }
  }

  return alerts;
}

// ============== Save Alerts ==============

export async function saveAlerts(alerts: Alert[], date: Date): Promise<void> {
  const dateStr = format(date, 'yyyy-MM-dd');

  for (const alert of alerts) {
    // Check if we already have this alert type for today
    const existing = await query(
      `SELECT 1 FROM alerts WHERE date = $1 AND alert_type = $2`,
      [dateStr, alert.type]
    );

    if (existing.rows.length === 0) {
      await query(
        `INSERT INTO alerts (date, alert_type, message, severity)
         VALUES ($1, $2, $3, $4)`,
        [dateStr, alert.type, alert.message, alert.severity]
      );
    }
  }
}

// ============== Discord/Slack Notifications ==============

export async function sendDiscordAlert(alert: Alert, webhookUrl: string): Promise<void> {
  const emoji =
    alert.severity === 'critical' ? '🔴' : alert.severity === 'warning' ? '🟠' : '🟢';

  try {
    await axios.post(webhookUrl, {
      content: `${emoji} **PXI Alert**\n${alert.message}`,
      embeds: [
        {
          color:
            alert.severity === 'critical'
              ? 0xff0000
              : alert.severity === 'warning'
              ? 0xff9900
              : 0x00ff00,
          fields: [
            ...(alert.score !== undefined
              ? [{ name: 'Score', value: alert.score.toFixed(1), inline: true }]
              : []),
            ...(alert.delta !== undefined
              ? [{ name: 'Change', value: `${alert.delta > 0 ? '+' : ''}${alert.delta.toFixed(1)}`, inline: true }]
              : []),
            { name: 'Time', value: new Date().toISOString(), inline: true },
          ],
        },
      ],
    });
  } catch (err: any) {
    console.error('Discord notification failed:', err.message);
  }
}

export async function sendSlackAlert(alert: Alert, webhookUrl: string): Promise<void> {
  const emoji =
    alert.severity === 'critical' ? ':red_circle:' : alert.severity === 'warning' ? ':large_orange_circle:' : ':large_green_circle:';

  try {
    await axios.post(webhookUrl, {
      text: `${emoji} *PXI Alert*\n${alert.message}`,
      attachments: [
        {
          color:
            alert.severity === 'critical'
              ? '#ff0000'
              : alert.severity === 'warning'
              ? '#ff9900'
              : '#00ff00',
          fields: [
            ...(alert.score !== undefined
              ? [{ title: 'Score', value: alert.score.toFixed(1), short: true }]
              : []),
            ...(alert.delta !== undefined
              ? [{ title: 'Change', value: `${alert.delta > 0 ? '+' : ''}${alert.delta.toFixed(1)}`, short: true }]
              : []),
          ],
        },
      ],
    });
  } catch (err: any) {
    console.error('Slack notification failed:', err.message);
  }
}

// ============== Process Alerts ==============

export async function processAlerts(date: Date): Promise<void> {
  console.log('\n🔔 Checking for alerts...\n');

  const alerts = await detectAlerts(date);

  if (alerts.length === 0) {
    console.log('  No alerts triggered.');
    return;
  }

  console.log(`  Found ${alerts.length} alert(s):`);

  for (const alert of alerts) {
    const icon =
      alert.severity === 'critical' ? '🔴' : alert.severity === 'warning' ? '🟠' : '🟢';
    console.log(`    ${icon} ${alert.message}`);
  }

  // Save to database
  await saveAlerts(alerts, date);

  // Send notifications
  const discordUrl = process.env.DISCORD_WEBHOOK_URL;
  const slackUrl = process.env.SLACK_WEBHOOK_URL;

  for (const alert of alerts) {
    if (discordUrl) await sendDiscordAlert(alert, discordUrl);
    if (slackUrl) await sendSlackAlert(alert, slackUrl);
  }

  console.log('\n  Alerts processed.\n');
}
