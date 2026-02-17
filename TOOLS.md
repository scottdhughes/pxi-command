# TOOLS.md - Local Notes

Skills define _how_ tools work. This file is for _your_ specifics — the stuff that's unique to your setup.

## What Goes Here

Things like:

- Camera names and locations
- SSH hosts and aliases
- Preferred voices for TTS
- Speaker/room names
- Device nicknames
- Anything environment-specific

## Examples

```markdown
### Cameras

- living-room → Main area, 180° wide angle
- front-door → Entrance, motion-triggered

### SSH

- home-server → 192.168.1.100, user: admin

### TTS

- Preferred voice: "Nova" (warm, slightly British)
- Default speaker: Kitchen HomePod
```

## Why Separate?

Skills are shared. Your setup is yours. Keeping them apart means you can update skills without losing your notes, and share skills without leaking your infrastructure.

## PXI Quant Ops

- Repo root: `/Users/scott/pxi`
- Signals worker: `/Users/scott/pxi/signals`
- Live endpoints:
  - `https://pxicommand.com/signals/latest`
  - `https://pxicommand.com/signals/api/runs`
- Core quality commands:
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npx wrangler deploy --env production`
- Scheduler expectations:
  - Mon/Tue/Wed at 15:00 UTC (with catch-up logic in scheduler)

### Quant Agent Jobs

- `quant-weekly-review` (Tuesdays 14:30 ET)
- `signals-freshness-watchdog` (daily 09:05 ET)

---

Add whatever helps you do your job. This is your cheat sheet.
