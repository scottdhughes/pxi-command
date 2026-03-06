import type { UtilityEventType } from './types'

const API_PRIMARY = 'https://api.pxicommand.com'
const API_ROLLBACK = 'https://pxi-api.novoamorx1.workers.dev'

let pinnedApiBase: string | null = null

function getApiUrlCandidates() {
  const configured = import.meta.env.VITE_API_URL?.trim()
  let candidates: string[]

  if (import.meta.env.DEV) {
    candidates = [configured || 'http://localhost:3000']
  } else if (configured) {
    candidates = configured === API_ROLLBACK ? [configured] : [configured, API_ROLLBACK]
  } else {
    candidates = [API_PRIMARY, API_ROLLBACK]
  }

  if (pinnedApiBase && candidates.includes(pinnedApiBase)) {
    return [pinnedApiBase, ...candidates.filter((candidate) => candidate !== pinnedApiBase)]
  }

  return candidates
}

export async function fetchApi(path: string, init?: RequestInit): Promise<Response> {
  const candidates = getApiUrlCandidates()
  let lastError: unknown

  for (let index = 0; index < candidates.length; index += 1) {
    const base = candidates[index]
    try {
      const response = await fetch(`${base}${path}`, init)
      pinnedApiBase = base
      return response
    } catch (error) {
      lastError = error
      if (index < candidates.length - 1) {
        console.warn(`API host unreachable (${base}), trying fallback`, error)
      }
    }
  }

  throw lastError instanceof Error ? lastError : new Error('API fetch failed')
}

function generateUtilitySessionId(): string {
  if (typeof window !== 'undefined' && typeof window.crypto !== 'undefined') {
    const bytes = new Uint8Array(10)
    window.crypto.getRandomValues(bytes)
    const token = Array.from(bytes).map((byte) => byte.toString(16).padStart(2, '0')).join('')
    return `ux_${token}`
  }

  return `ux_${Math.random().toString(36).slice(2, 18)}`
}

export function getOrCreateUtilitySessionId(): string {
  if (typeof window === 'undefined') {
    return generateUtilitySessionId()
  }

  try {
    const existing = window.localStorage.getItem('pxi_utility_session_id')
    if (existing && /^ux_[A-Za-z0-9_-]{10,}$/.test(existing)) {
      return existing
    }

    const next = generateUtilitySessionId()
    window.localStorage.setItem('pxi_utility_session_id', next)
    return next
  } catch {
    return generateUtilitySessionId()
  }
}

export function utilityDecisionEventForState(
  state: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION' | null | undefined,
): UtilityEventType {
  if (state === 'ACTIONABLE') return 'decision_actionable_view'
  if (state === 'NO_ACTION') return 'decision_no_action_view'
  return 'decision_watch_view'
}
