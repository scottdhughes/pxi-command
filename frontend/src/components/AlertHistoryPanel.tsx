import { useState } from 'react'

import type { AlertData } from '../lib/types'

export function AlertHistoryPanel({
  alerts,
  accuracy,
  onClose,
  inPage = false,
}: {
  alerts: AlertData[]
  accuracy: Record<string, { total: number; accuracy_7d: number | null; avg_return_7d: number }>
  onClose: () => void
  inPage?: boolean
}) {
  const [filterType, setFilterType] = useState<string | null>(null)

  const filteredAlerts = filterType
    ? alerts.filter((alert) => alert.alert_type === filterType)
    : alerts

  const alertTypes = [...new Set(alerts.map((alert) => alert.alert_type))]

  const getSeverityStyles = (severity: string) => {
    switch (severity) {
      case 'critical':
        return 'border-[#ff6b6b]/40 bg-[#ff6b6b]/5'
      case 'warning':
        return 'border-[#f59e0b]/40 bg-[#f59e0b]/5'
      default:
        return 'border-[#00a3ff]/20 bg-[#00a3ff]/5'
    }
  }

  const getSeverityDot = (severity: string) => {
    switch (severity) {
      case 'critical':
        return 'bg-[#ff6b6b]'
      case 'warning':
        return 'bg-[#f59e0b]'
      default:
        return 'bg-[#00a3ff]'
    }
  }

  const formatReturn = (value: number | null) => {
    if (value === null) return '—'
    const color = value >= 0 ? 'text-[#00c896]' : 'text-[#ff6b6b]'
    return <span className={color}>{value >= 0 ? '+' : ''}{value.toFixed(2)}%</span>
  }

  const formatType = (type: string) =>
    type.replace(/_/g, ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())

  const containerClass = inPage
    ? 'min-h-screen bg-black text-[#f3f3f3] px-4 pt-8 pb-10'
    : 'fixed inset-0 bg-black/90 z-50 overflow-y-auto'

  return (
    <div className={containerClass}>
      <div className={inPage ? 'max-w-4xl mx-auto' : 'min-h-screen px-4 py-8 max-w-4xl mx-auto'}>
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-[#f3f3f3] text-lg font-light tracking-wider">ALERT HISTORY</h2>
            <p className="text-[#949ba5]/60 text-[10px] tracking-wide mt-1">
              Historical alerts with forward return analysis
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-[#949ba5] hover:text-[#f3f3f3] transition-colors p-2"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {Object.keys(accuracy).length > 0 && (
          <div className="mb-6 p-4 bg-[#0a0a0a]/80 border border-[#1a1a1a] rounded-lg">
            <h3 className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-3">
              Alert Accuracy by Type
            </h3>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
              {Object.entries(accuracy).map(([type, stats]) => (
                <div key={type} className="bg-[#0f0f0f] rounded px-3 py-2 border border-[#1a1a1a]">
                  <div className="text-[10px] text-[#949ba5] mb-1">{formatType(type)}</div>
                  <div className="flex items-center gap-2">
                    {stats.accuracy_7d !== null ? (
                      <span className={`text-sm font-mono ${stats.accuracy_7d >= 60 ? 'text-[#00c896]' : stats.accuracy_7d >= 50 ? 'text-[#f59e0b]' : 'text-[#ff6b6b]'}`}>
                        {Math.round(stats.accuracy_7d)}%
                      </span>
                    ) : (
                      <span className="text-sm font-mono text-[#949ba5]/50">—</span>
                    )}
                    <span className="text-[8px] text-[#949ba5]/40">n={stats.total}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="flex flex-wrap gap-2 mb-4">
          <button
            onClick={() => setFilterType(null)}
            className={`px-3 py-1 text-[10px] uppercase tracking-wider rounded transition-colors ${
              !filterType
                ? 'bg-[#00a3ff]/20 text-[#00a3ff] border border-[#00a3ff]/30'
                : 'bg-[#1a1a1a] text-[#949ba5] border border-[#26272b] hover:border-[#949ba5]/30'
            }`}
          >
            All
          </button>
          {alertTypes.map((type) => (
            <button
              key={type}
              onClick={() => setFilterType(type)}
              className={`px-3 py-1 text-[10px] uppercase tracking-wider rounded transition-colors ${
                filterType === type
                  ? 'bg-[#00a3ff]/20 text-[#00a3ff] border border-[#00a3ff]/30'
                  : 'bg-[#1a1a1a] text-[#949ba5] border border-[#26272b] hover:border-[#949ba5]/30'
              }`}
            >
              {formatType(type)}
            </button>
          ))}
        </div>

        <div className="space-y-2">
          {filteredAlerts.length === 0 ? (
            <div className="text-center py-12 text-[#949ba5]/50 text-sm">
              No alerts found
            </div>
          ) : (
            filteredAlerts.map((alert) => (
              <div
                key={alert.id}
                className={`p-4 border rounded-lg ${getSeverityStyles(alert.severity)}`}
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <span className={`w-2 h-2 rounded-full ${getSeverityDot(alert.severity)}`} />
                      <span className="text-[10px] text-[#949ba5]/60 uppercase tracking-wider">
                        {formatType(alert.alert_type)}
                      </span>
                      <span className="text-[9px] text-[#949ba5]/40">
                        {new Date(alert.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                      </span>
                    </div>
                    <p className="text-[#f3f3f3] text-sm leading-relaxed">
                      {alert.message}
                    </p>
                    {alert.pxi_score !== null && (
                      <div className="mt-2 text-[10px] text-[#949ba5]/60">
                        PXI at alert: <span className="text-[#00a3ff] font-mono">{alert.pxi_score.toFixed(1)}</span>
                      </div>
                    )}
                  </div>
                  {(alert.forward_return_7d !== null || alert.forward_return_30d !== null) && (
                    <div className="flex-shrink-0 text-right">
                      <div className="text-[8px] text-[#949ba5]/40 uppercase tracking-wider mb-1">
                        Forward Returns
                      </div>
                      {alert.forward_return_7d !== null && (
                        <div className="text-[10px]">
                          <span className="text-[#949ba5]/50">7d:</span> {formatReturn(alert.forward_return_7d)}
                        </div>
                      )}
                      {alert.forward_return_30d !== null && (
                        <div className="text-[10px]">
                          <span className="text-[#949ba5]/50">30d:</span> {formatReturn(alert.forward_return_30d)}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            ))
          )}
        </div>

        <div className="mt-8 p-4 border border-dashed border-[#26272b] rounded-lg text-center">
          <p className="text-[#949ba5]/60 text-sm mb-2">
            Get alerts delivered to your inbox
          </p>
          <button className="px-4 py-2 bg-[#00a3ff]/10 border border-[#00a3ff]/30 rounded text-[10px] text-[#00a3ff] uppercase tracking-wider hover:bg-[#00a3ff]/20 transition-colors">
            Coming Soon
          </button>
        </div>
      </div>
    </div>
  )
}
