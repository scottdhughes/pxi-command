import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'

import { fetchApi } from '../../lib/api'
import { formatUnavailableReason } from '../../lib/display'
import type {
  CategoryDetailData,
  HistoryDataPoint,
  MLAccuracyData,
  PXIData,
} from '../../lib/types'

function Tooltip({ children, content }: { children: ReactNode; content: string }) {
  const [show, setShow] = useState(false)

  return (
    <span
      className="relative inline-flex items-center cursor-help"
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
    >
      {children}
      <span className="ml-1 text-[#949ba5]/40 text-[8px]">ⓘ</span>
      {show && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-[#1a1a1a] border border-[#26272b] rounded text-[10px] text-[#949ba5] whitespace-nowrap z-50 shadow-lg">
          {content}
          <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-[#1a1a1a]" />
        </span>
      )}
    </span>
  )
}

export function HistoricalChart({
  data,
  range,
  onRangeChange,
}: {
  data: HistoryDataPoint[]
  range: '7d' | '30d' | '90d'
  onRangeChange: (range: '7d' | '30d' | '90d') => void
}) {
  const [hoveredPoint, setHoveredPoint] = useState<HistoryDataPoint | null>(null)
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)

  if (!data || data.length < 2) return null

  const rangeMap = { '7d': 7, '30d': 30, '90d': 90 }
  const displayData = data.slice(-rangeMap[range])

  if (displayData.length < 2) return null

  const min = Math.min(...displayData.map((d) => d.score))
  const max = Math.max(...displayData.map((d) => d.score))
  const scoreRange = max - min || 1

  const width = 100
  const height = 100
  const padding = { top: 8, right: 4, bottom: 20, left: 4 }
  const chartWidth = width - padding.left - padding.right
  const chartHeight = height - padding.top - padding.bottom

  const getRegimeColor = (regime?: string) => {
    switch (regime) {
      case 'RISK_ON':
        return '#00a3ff'
      case 'RISK_OFF':
        return '#ff6b6b'
      case 'TRANSITION':
        return '#f59e0b'
      default:
        return '#949ba5'
    }
  }

  const points = displayData.map((d, i) => {
    const x = padding.left + (i / (displayData.length - 1)) * chartWidth
    const y = padding.top + chartHeight - ((d.score - min) / scoreRange) * chartHeight
    return { x, y, data: d }
  })

  const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x},${p.y}`).join(' ')
  const areaD = `${pathD} L${points[points.length - 1].x},${height - padding.bottom} L${points[0].x},${height - padding.bottom} Z`

  return (
    <div className="w-full mt-8">
      <div className="flex justify-between items-center mb-4">
        <div className="text-[10px] text-[#949ba5]/50 uppercase tracking-widest">
          Historical Trend
        </div>
        <div className="flex gap-1">
          {(['7d', '30d', '90d'] as const).map((r) => (
            <button
              key={r}
              onClick={() => onRangeChange(r)}
              className={`px-2 py-0.5 text-[9px] uppercase tracking-wider rounded transition-all ${
                range === r
                  ? 'bg-[#00a3ff]/20 text-[#00a3ff] border border-[#00a3ff]/30'
                  : 'text-[#949ba5]/50 hover:text-[#949ba5] border border-transparent'
              }`}
            >
              {r}
            </button>
          ))}
        </div>
      </div>

      <div className="relative bg-[#0a0a0a]/50 rounded-lg border border-[#1a1a1a] p-3">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="w-full h-32"
          preserveAspectRatio="none"
          onMouseLeave={() => {
            setHoveredPoint(null)
            setHoveredIndex(null)
          }}
        >
          <defs>
            <linearGradient id="historyGradient" x1="0%" y1="0%" x2="100%" y2="0%">
              {displayData.map((d, i) => (
                <stop
                  key={i}
                  offset={`${(i / (displayData.length - 1)) * 100}%`}
                  stopColor={getRegimeColor(d.regime)}
                />
              ))}
            </linearGradient>

            <linearGradient id="areaGradient" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stopColor="#00a3ff" stopOpacity="0.15" />
              <stop offset="100%" stopColor="#00a3ff" stopOpacity="0" />
            </linearGradient>
          </defs>

          {[25, 50, 75].map((v) => {
            const y = padding.top + chartHeight - ((v - min) / scoreRange) * chartHeight
            if (y < padding.top || y > height - padding.bottom) return null
            return (
              <line
                key={v}
                x1={padding.left}
                y1={y}
                x2={width - padding.right}
                y2={y}
                stroke="#26272b"
                strokeWidth="0.5"
                strokeDasharray="2,2"
              />
            )
          })}

          <path d={areaD} fill="url(#areaGradient)" />

          <path
            d={pathD}
            fill="none"
            stroke="url(#historyGradient)"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {points.map((p, i) => (
            <g key={i}>
              <rect
                x={p.x - chartWidth / displayData.length / 2}
                y={padding.top}
                width={chartWidth / displayData.length}
                height={chartHeight}
                fill="transparent"
                onMouseEnter={() => {
                  setHoveredPoint(p.data)
                  setHoveredIndex(i)
                }}
              />
              {hoveredIndex === i && (
                <>
                  <line
                    x1={p.x}
                    y1={padding.top}
                    x2={p.x}
                    y2={height - padding.bottom}
                    stroke="#26272b"
                    strokeWidth="1"
                  />
                  <circle
                    cx={p.x}
                    cy={p.y}
                    r="3"
                    fill={getRegimeColor(p.data.regime)}
                    stroke="#0a0a0a"
                    strokeWidth="1.5"
                  />
                </>
              )}
            </g>
          ))}
        </svg>

        {hoveredPoint && hoveredIndex !== null && (
          <div className="absolute top-2 left-1/2 -translate-x-1/2 bg-[#1a1a1a] border border-[#26272b] rounded px-3 py-2 text-center shadow-lg pointer-events-none z-10">
            <div className="text-[9px] text-[#949ba5]/60 mb-1">
              {new Date(hoveredPoint.date + 'T12:00:00').toLocaleDateString('en-US', {
                month: 'short',
                day: 'numeric',
                year: displayData.length > 60 ? '2-digit' : undefined,
              })}
            </div>
            <div className="text-lg font-light text-[#f3f3f3]">
              {Math.round(hoveredPoint.score)}
            </div>
            {hoveredPoint.regime && (
              <div
                className="text-[8px] uppercase tracking-wider mt-1"
                style={{ color: getRegimeColor(hoveredPoint.regime) }}
              >
                {hoveredPoint.regime.replace('_', ' ')}
              </div>
            )}
          </div>
        )}

        <div className="flex justify-between mt-3 pt-3 border-t border-[#1a1a1a]">
          <div className="text-center">
            <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider">Low</div>
            <div className="text-[11px] font-mono text-[#949ba5]">{Math.round(min)}</div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider">Avg</div>
            <div className="text-[11px] font-mono text-[#949ba5]">
              {Math.round(displayData.reduce((a, b) => a + b.score, 0) / displayData.length)}
            </div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider">High</div>
            <div className="text-[11px] font-mono text-[#949ba5]">{Math.round(max)}</div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider">Change</div>
            <div className={`text-[11px] font-mono ${
              displayData[displayData.length - 1].score >= displayData[0].score
                ? 'text-[#00a3ff]'
                : 'text-[#ff6b6b]'
            }`}>
              {displayData[displayData.length - 1].score >= displayData[0].score ? '+' : ''}
              {(displayData[displayData.length - 1].score - displayData[0].score).toFixed(1)}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export function StaleDataWarning({ freshness }: { freshness: PXIData['dataFreshness'] }) {
  const [expanded, setExpanded] = useState(false)

  if (!freshness) return null

  const topOffenders = freshness.topOffenders || []
  const hasOffenders = freshness.hasStaleData || topOffenders.length > 0

  if (!hasOffenders) return null

  return (
    <div className="w-full mt-4">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-center gap-2 py-2 px-4 bg-[#f59e0b]/10 border border-[#f59e0b]/30 rounded text-[10px] text-[#f59e0b] uppercase tracking-wider hover:bg-[#f59e0b]/20 transition-colors"
      >
        <span>operator panel</span>
        <span>{freshness.staleCount} stale input{freshness.staleCount === 1 ? '' : 's'}</span>
        <span className="text-[#f59e0b]/50">{expanded ? '▲' : '▼'}</span>
      </button>
      {expanded && (
        <div className="mt-2 p-3 bg-[#0a0a0a]/60 border border-[#26272b] rounded">
          <div className="flex flex-wrap gap-3 text-[9px] text-[#949ba5]/70 uppercase tracking-wider mb-3">
            <div>
              last refresh {freshness.lastRefreshAtUtc ? new Date(freshness.lastRefreshAtUtc).toLocaleString() : 'unknown'}
              {freshness.lastRefreshSource ? ` (${freshness.lastRefreshSource.replace(/_/g, ' ')})` : ''}
            </div>
            <div>
              next refresh {freshness.nextExpectedRefreshAtUtc ? new Date(freshness.nextExpectedRefreshAtUtc).toLocaleString() : 'unknown'}
              {typeof freshness.nextExpectedRefreshInMinutes === 'number' ? ` (${freshness.nextExpectedRefreshInMinutes}m)` : ''}
            </div>
          </div>
          <div className="space-y-2">
            {(topOffenders.length > 0 ? topOffenders : freshness.staleIndicators.slice(0, 3).map((s) => ({
              id: s.id,
              lastUpdate: s.lastUpdate,
              daysOld: s.daysOld,
              maxAgeDays: 0,
              chronic: false,
              owner: 'market_data' as const,
              escalation: 'observe' as const,
            }))).map((ind) => (
              <div key={ind.id} className="border border-[#26272b] rounded px-2 py-2">
                <div className="flex justify-between items-center gap-2 text-[10px]">
                  <span className="text-[#d7dbe1]">{ind.id.replace(/_/g, ' ')}</span>
                  <span className="text-[#f59e0b]/80">
                    {ind.daysOld === null ? 'unknown age' : `${ind.daysOld}d old`}
                    {ind.maxAgeDays > 0 ? ` / max ${ind.maxAgeDays}d` : ''}
                  </span>
                </div>
                <div className="mt-1 flex flex-wrap gap-2 text-[8px] uppercase tracking-wider">
                  <span className="px-1.5 py-0.5 border border-[#26272b] rounded text-[#949ba5]">{ind.owner}</span>
                  <span className="px-1.5 py-0.5 border border-[#26272b] rounded text-[#949ba5]">{ind.escalation.replace(/_/g, ' ')}</span>
                  {ind.chronic && (
                    <span className="px-1.5 py-0.5 border border-[#ff6b6b]/40 rounded text-[#ff6b6b]">chronic</span>
                  )}
                </div>
                {ind.lastUpdate && (
                  <div className="mt-1 text-[8px] text-[#949ba5]/60">
                    last update {new Date(ind.lastUpdate).toLocaleDateString()}
                  </div>
                )}
              </div>
            ))}
          </div>
          {freshness.staleCount > 3 && (
            <div className="text-[8px] text-[#949ba5]/40 mt-2">
              +{freshness.staleCount - 3} more stale indicators
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export function MLAccuracyBadge({ accuracy }: { accuracy: MLAccuracyData | null }) {
  if (!accuracy) return null

  const acc7d = accuracy.rolling_7d.direction_accuracy
  const acc30d = accuracy.rolling_30d.direction_accuracy
  const insufficientSample = accuracy.coverage_quality === 'INSUFFICIENT' || accuracy.coverage.evaluated_count < accuracy.minimum_reliable_sample

  const getColor = (acc: number | null) => {
    if (insufficientSample) return 'text-[#949ba5]'
    if (acc === null) return 'text-[#949ba5]'
    if (acc >= 70) return 'text-[#00c896]'
    if (acc >= 55) return 'text-[#f59e0b]'
    return 'text-[#ff6b6b]'
  }

  const renderValue = (acc: number | null) => (acc === null ? 'N/A' : `${Math.round(acc)}%`)

  return (
    <div className="flex justify-center gap-4 mt-3 mb-2">
      <Tooltip content="Rolling 30-day directional accuracy of ML predictions">
        <div className="flex items-center gap-2 bg-[#0a0a0a]/60 rounded px-2.5 py-1 border border-[#1a1a1a]">
          <span className="text-[8px] text-[#949ba5]/50 uppercase tracking-wider">7d Acc</span>
          <span className={`text-[10px] font-mono ${getColor(acc7d)}`}>
            {renderValue(acc7d)}
          </span>
        </div>
      </Tooltip>
      <Tooltip content="Rolling 90-day directional accuracy of ML predictions">
        <div className="flex items-center gap-2 bg-[#0a0a0a]/60 rounded px-2.5 py-1 border border-[#1a1a1a]">
          <span className="text-[8px] text-[#949ba5]/50 uppercase tracking-wider">30d Acc</span>
          <span className={`text-[10px] font-mono ${getColor(acc30d)}`}>
            {renderValue(acc30d)}
          </span>
        </div>
      </Tooltip>
      {accuracy.unavailable_reasons.length > 0 && (
        <div className="text-[8px] text-[#949ba5]/60 self-center">
          {accuracy.unavailable_reasons.slice(0, 1).map((reason) => formatUnavailableReason(reason)).join(', ')}
        </div>
      )}
      {insufficientSample && (
        <div className="text-[8px] text-[#949ba5]/60 self-center">
          low sample (n={accuracy.coverage.evaluated_count}/{accuracy.minimum_reliable_sample})
        </div>
      )}
    </div>
  )
}

export function CategoryModal({
  category,
  onClose,
}: {
  category: string
  onClose: () => void
}) {
  const [data, setData] = useState<CategoryDetailData | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetchApi(`/api/category/${category}`)
        if (res.ok) {
          const json = await res.json()
          setData(json)
        }
      } catch (err) {
        console.error('Failed to fetch category details:', err)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [category])

  const formatDisplayName = (name: string) =>
    name.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase())

  const renderSparkline = () => {
    if (!data || data.history.length < 2) return null

    const values = data.history.map((h) => h.score)
    const min = Math.min(...values)
    const max = Math.max(...values)
    const scoreRange = max - min || 1
    const width = 320
    const height = 80

    const points = data.history.map((h, i) => {
      const x = (i / (data.history.length - 1)) * width
      const y = height - ((h.score - min) / scoreRange) * (height - 10) - 5
      return `${x},${y}`
    }).join(' ')

    const currentScore = data.score
    const avgScore = values.reduce((a, b) => a + b, 0) / values.length

    return (
      <div className="mb-6">
        <div className="flex justify-between items-center mb-2">
          <span className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider">90-Day History</span>
          <span className="text-[10px] text-[#949ba5]/60">
            Avg: <span className="font-mono text-[#f3f3f3]">{avgScore.toFixed(1)}</span>
          </span>
        </div>
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-20">
          <defs>
            <linearGradient id={`gradient-${category}`} x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stopColor="#00a3ff" stopOpacity="0.2" />
              <stop offset="100%" stopColor="#00a3ff" stopOpacity="0" />
            </linearGradient>
          </defs>
          <polygon
            points={`0,${height} ${points} ${width},${height}`}
            fill={`url(#gradient-${category})`}
          />
          <polyline
            fill="none"
            stroke="#00a3ff"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            points={points}
          />
          {data.history.length > 0 && (
            <circle
              cx={width}
              cy={height - ((currentScore - min) / scoreRange) * (height - 10) - 5}
              r="4"
              fill="#00a3ff"
            />
          )}
        </svg>
        <div className="flex justify-between text-[9px] text-[#949ba5]/40 mt-1">
          <span>{data.history[0]?.date}</span>
          <span>{data.history[data.history.length - 1]?.date}</span>
        </div>
      </div>
    )
  }

  const getScoreColor = (normalized: number) => {
    if (normalized >= 70) return 'text-[#00a3ff]'
    if (normalized >= 50) return 'text-[#f3f3f3]'
    if (normalized >= 30) return 'text-[#f59e0b]'
    return 'text-[#ff6b6b]'
  }

  return (
    <div className="fixed inset-0 bg-black/90 z-50 flex items-center justify-center p-4">
      <div className="bg-[#0a0a0a] border border-[#26272b] rounded-lg max-w-md w-full max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between p-4 border-b border-[#26272b]">
          <div>
            <h2 className="text-[#f3f3f3] text-lg font-light tracking-wider uppercase">
              {formatDisplayName(category)}
            </h2>
            <p className="text-[#949ba5]/60 text-[10px] tracking-wide mt-0.5">
              Category Deep Dive
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

        <div className="p-4">
          {loading ? (
            <div className="text-center py-8 text-[#949ba5]/50 text-sm">Loading...</div>
          ) : data ? (
            <>
              <div className="flex items-center justify-between mb-6">
                <div>
                  <div className="text-4xl font-light text-[#f3f3f3]">{Math.round(data.score)}</div>
                  <div className="text-[10px] text-[#949ba5]/60 mt-1">
                    Weight: {(data.weight * 100).toFixed(0)}%
                  </div>
                </div>
                <div className="text-right">
                  <div className={`text-2xl font-mono ${data.percentile_rank >= 70 ? 'text-[#00a3ff]' : data.percentile_rank <= 30 ? 'text-[#ff6b6b]' : 'text-[#949ba5]'}`}>
                    {data.percentile_rank}<span className="text-sm">th</span>
                  </div>
                  <div className="text-[10px] text-[#949ba5]/60 mt-1">
                    90-day percentile
                  </div>
                </div>
              </div>

              {renderSparkline()}

              <div>
                <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-3">
                  Component Indicators
                </div>
                <div className="space-y-2">
                  {data.indicators.map((ind) => (
                    <div key={ind.id} className="flex items-center justify-between py-2 border-b border-[#1a1a1a]">
                      <div>
                        <div className="text-[11px] text-[#f3f3f3]">{ind.name}</div>
                        <div className="text-[9px] text-[#949ba5]/50 font-mono mt-0.5">
                          Raw: {typeof ind.raw_value === 'number' ? ind.raw_value.toFixed(2) : '—'}
                        </div>
                      </div>
                      <div className="text-right">
                        <div className={`text-lg font-mono ${getScoreColor(ind.normalized_value)}`}>
                          {Math.round(ind.normalized_value)}
                        </div>
                        <div className="w-12 h-1 bg-[#26272b] rounded-full overflow-hidden mt-1">
                          <div
                            className="h-full bg-[#00a3ff]/60 rounded-full"
                            style={{ width: `${ind.normalized_value}%` }}
                          />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="mt-6 p-3 bg-[#0f0f0f] border border-[#1a1a1a] rounded">
                <div className="text-[10px] text-[#949ba5]">
                  <span className="text-[#00a3ff]">{formatDisplayName(category)}</span> at{' '}
                  <span className="font-mono text-[#f3f3f3]">{Math.round(data.score)}</span> is in the{' '}
                  <span className={`font-mono ${data.percentile_rank >= 70 ? 'text-[#00a3ff]' : data.percentile_rank <= 30 ? 'text-[#ff6b6b]' : 'text-[#f3f3f3]'}`}>
                    {data.percentile_rank}th percentile
                  </span>{' '}
                  of the last 90 days.
                  {data.percentile_rank >= 80 && ' This is an elevated reading.'}
                  {data.percentile_rank <= 20 && ' This is a depressed reading.'}
                </div>
              </div>
            </>
          ) : (
            <div className="text-center py-8 text-[#949ba5]/50 text-sm">Failed to load data</div>
          )}
        </div>
      </div>
    </div>
  )
}
