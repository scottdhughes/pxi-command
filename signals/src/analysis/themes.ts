export interface ThemeDefinition {
  theme_id: string
  display_name: string
  keywords: string[]
  seed_tickers: string[]
  proxy_etf?: string
  risk_keywords?: string[]
}

export const THEMES: ThemeDefinition[] = [
  {
    theme_id: "nuclear_uranium",
    display_name: "Nuclear / Uranium",
    keywords: ["nuclear", "uranium", "reactor", "fuel cycle", "smr", "small modular"],
    seed_tickers: ["CCJ", "UEC", "UUUU", "URNM"],
    proxy_etf: "URNM",
    risk_keywords: ["regulatory", "delay", "capex"],
  },
  {
    theme_id: "utilities_grid",
    display_name: "Utilities / Grid",
    keywords: ["utilities", "grid", "transmission", "distribution", "power line", "substation"],
    seed_tickers: ["XLU", "NEE", "DUK", "PWR"],
    proxy_etf: "XLU",
  },
  {
    theme_id: "space_satellites",
    display_name: "Space / Satellites",
    keywords: ["satellite", "launch", "orbital", "space", "payload", "constellation"],
    seed_tickers: ["RKLB", "LMT", "NOC", "SPCE"],
  },
  {
    theme_id: "defense_aerospace",
    display_name: "Defense / Aerospace",
    keywords: ["defense", "aerospace", "missile", "fighter", "contract award"],
    seed_tickers: ["LMT", "RTX", "NOC", "GD"],
  },
  {
    theme_id: "copper_critical",
    display_name: "Copper / Critical Minerals",
    keywords: ["copper", "critical minerals", "lithium", "rare earth", "mining"],
    seed_tickers: ["FCX", "COPX", "LIT"],
  },
  {
    theme_id: "financials_regional",
    display_name: "Financials (Regional Banks / Insurance)",
    keywords: ["regional bank", "insurance", "deposit", "net interest", "credit quality"],
    seed_tickers: ["KRE", "PNC", "USB", "CB"],
  },
  {
    theme_id: "industrials_infra",
    display_name: "Industrials / Infrastructure",
    keywords: ["infrastructure", "construction", "equipment", "capex", "reshoring"],
    seed_tickers: ["CAT", "DE", "PAVE"],
  },
  {
    theme_id: "gold_miners",
    display_name: "Gold Miners",
    keywords: ["gold miners", "gold", "bullion", "hedge"],
    seed_tickers: ["GDX", "NEM", "GOLD"],
  },
  {
    theme_id: "midstream_energy",
    display_name: "Midstream / Energy Infrastructure",
    keywords: ["midstream", "pipeline", "lng", "terminal", "energy infrastructure"],
    seed_tickers: ["KMI", "ENB", "EPD"],
  },
  {
    theme_id: "automation_rpa",
    display_name: "Automation / RPA",
    keywords: ["automation", "rpa", "productivity", "ai workflow", "software automation"],
    seed_tickers: ["PATH", "MSFT", "ADBE"],
  },
]
