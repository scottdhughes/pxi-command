export type AppRoute = '/' | '/spec' | '/alerts' | '/guide' | '/brief' | '/opportunities' | '/inbox'

interface RouteMeta {
  title: string
  description: string
  canonical: string
  ogTitle: string
  ogDescription: string
  jsonLd: Record<string, unknown>
}

const ROUTE_META: Record<AppRoute, RouteMeta> = {
  '/': {
    title: 'PXI /COMMAND - Macro Market Strength Index',
    description: 'Real-time composite macro indicator synthesizing volatility, credit spreads, market breadth, positioning, and global risk signals into a single 0-100 score.',
    canonical: 'https://pxicommand.com/',
    ogTitle: 'PXI /COMMAND - Macro Market Strength Index',
    ogDescription: 'Real-time composite macro indicator: volatility, credit, breadth, positioning, and global risk signals in a single 0-100 score.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebSite',
      name: 'PXI /COMMAND',
      url: 'https://pxicommand.com/',
      description: 'Real-time macro market strength index',
      potentialAction: {
        '@type': 'SearchAction',
        target: 'https://pxicommand.com/?q={search_term_string}',
        'query-input': 'required name=search_term_string',
      },
    },
  },
  '/spec': {
    title: 'PXI /COMMAND Protocol Specification',
    description: 'PXI methodology, indicators, and risk architecture. Learn the full framework behind the macro market strength index.',
    canonical: 'https://pxicommand.com/spec',
    ogTitle: 'PXI /COMMAND Protocol Specification',
    ogDescription: 'Detailed PXI methodology, category weights, and modeling assumptions for transparency.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI /COMMAND Protocol Specification',
      url: 'https://pxicommand.com/spec',
      description: 'Detailed explanation of PXI indicators, methodology, and backtest assumptions.',
    },
  },
  '/alerts': {
    title: 'PXI /COMMAND Alert History',
    description: 'Historical alert signal log for PXI with forward return attribution and regime-specific accuracy context.',
    canonical: 'https://pxicommand.com/alerts',
    ogTitle: 'PXI /COMMAND Alert History',
    ogDescription: 'Review historical divergence alerts and outcome attribution from the PXI /COMMAND index.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI /COMMAND Alert History',
      url: 'https://pxicommand.com/alerts',
      description: 'Historical divergence alerts and regime-based risk signal archive.',
    },
  },
  '/guide': {
    title: 'PXI /COMMAND Guide',
    description: 'How to interpret PXI score levels, regimes, and allocation guidance for macro and risk environments.',
    canonical: 'https://pxicommand.com/guide',
    ogTitle: 'PXI /COMMAND Guide',
    ogDescription: 'A practical guide to reading PXI scores and regime signals.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'HowTo',
      name: 'PXI /COMMAND Guide',
      url: 'https://pxicommand.com/guide',
      description: 'How to interpret PXI scores, regimes, and risk guidance.',
      step: [],
    },
  },
  '/brief': {
    title: 'PXI Daily Brief',
    description: 'Daily market brief with regime delta, explainability movers, and data freshness context.',
    canonical: 'https://pxicommand.com/brief',
    ogTitle: 'PXI Daily Brief',
    ogDescription: 'Daily macro brief: what changed, why it matters, and current risk posture.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI Daily Brief',
      url: 'https://pxicommand.com/brief',
      description: 'Daily PXI brief with explainability and freshness context.',
    },
  },
  '/opportunities': {
    title: 'PXI Opportunities',
    description: 'Ranked market opportunities blending PXI, ML, and Signals theme context.',
    canonical: 'https://pxicommand.com/opportunities',
    ogTitle: 'PXI Opportunities',
    ogDescription: 'Deterministic opportunity feed with conviction, rationale, and historical hit-rate context.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI Opportunities',
      url: 'https://pxicommand.com/opportunities',
      description: 'Opportunity feed for 7d and 30d horizons.',
    },
  },
  '/inbox': {
    title: 'PXI Alert Inbox',
    description: 'Market-wide alert feed with severity, event metadata, and email digest subscription.',
    canonical: 'https://pxicommand.com/inbox',
    ogTitle: 'PXI Alert Inbox',
    ogDescription: 'In-app feed for regime changes, threshold events, opportunity spikes, and freshness warnings.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI Alert Inbox',
      url: 'https://pxicommand.com/inbox',
      description: 'In-app market alerts feed for PXI.',
    },
  },
}

export function normalizeRoute(pathname: string): AppRoute {
  const path = (pathname || '/').replace(/\/+$/, '')
  if (path === '/spec') return '/spec'
  if (path === '/alerts') return '/alerts'
  if (path === '/guide') return '/guide'
  if (path === '/brief') return '/brief'
  if (path === '/opportunities') return '/opportunities'
  if (path === '/inbox') return '/inbox'
  return '/'
}

function setMeta(name: string, content: string, attr: 'name' | 'property' = 'name') {
  const selector = attr === 'name' ? `meta[name="${name}"]` : `meta[property="${name}"]`
  let tag = document.querySelector(selector) as HTMLMetaElement | null
  if (!tag) {
    tag = document.createElement('meta')
    tag.setAttribute(attr, name)
    document.head.appendChild(tag)
  }
  tag.setAttribute('content', content)
}

function setJsonLd(meta: Record<string, unknown>) {
  let tag = document.getElementById('route-jsonld') as HTMLScriptElement | null
  if (!tag) {
    tag = document.createElement('script')
    tag.setAttribute('type', 'application/ld+json')
    tag.id = 'route-jsonld'
    document.head.appendChild(tag)
  }
  tag.text = JSON.stringify(meta, null, 2)
}

export function applyRouteMetadata(route: AppRoute) {
  const meta = ROUTE_META[route]
  document.title = meta.title

  const setCanonical = (href: string) => {
    let tag = document.querySelector('link[rel="canonical"]') as HTMLLinkElement | null
    if (!tag) {
      tag = document.createElement('link')
      tag.setAttribute('rel', 'canonical')
      document.head.appendChild(tag)
    }
    tag.setAttribute('href', href)
  }

  setMeta('description', meta.description, 'name')
  setMeta('og:title', meta.ogTitle, 'property')
  setMeta('og:description', meta.ogDescription, 'property')
  setMeta('og:url', meta.canonical, 'property')
  setMeta('twitter:title', meta.ogTitle, 'name')
  setMeta('twitter:description', meta.ogDescription, 'name')
  setCanonical(meta.canonical)
  setJsonLd(meta.jsonLd)
}

export interface RouteFetchPlan {
  poll: boolean
  pxi: boolean
  signal: boolean
  plan: boolean
  prediction: boolean
  ensemble: boolean
  accuracy: boolean
  history: boolean
  alertsHistory: boolean
  brief: boolean
  opportunities: boolean
  inbox: boolean
  signalsDetail: boolean
  similar: boolean
  backtest: boolean
}

const EMPTY_FETCH_PLAN: RouteFetchPlan = {
  poll: false,
  pxi: false,
  signal: false,
  plan: false,
  prediction: false,
  ensemble: false,
  accuracy: false,
  history: false,
  alertsHistory: false,
  brief: false,
  opportunities: false,
  inbox: false,
  signalsDetail: false,
  similar: false,
  backtest: false,
}

export function getRouteFetchPlan(route: AppRoute): RouteFetchPlan {
  if (route === '/') {
    return {
      poll: true,
      pxi: true,
      signal: true,
      plan: true,
      prediction: true,
      ensemble: true,
      accuracy: true,
      history: true,
      alertsHistory: true,
      brief: true,
      opportunities: true,
      inbox: true,
      signalsDetail: true,
      similar: true,
      backtest: true,
    }
  }

  if (route === '/alerts') {
    return {
      ...EMPTY_FETCH_PLAN,
      poll: true,
      alertsHistory: true,
    }
  }

  if (route === '/brief') {
    return {
      ...EMPTY_FETCH_PLAN,
      poll: true,
      brief: true,
    }
  }

  if (route === '/opportunities') {
    return {
      ...EMPTY_FETCH_PLAN,
      poll: true,
      opportunities: true,
    }
  }

  if (route === '/inbox') {
    return {
      ...EMPTY_FETCH_PLAN,
      poll: true,
      inbox: true,
    }
  }

  return EMPTY_FETCH_PLAN
}
