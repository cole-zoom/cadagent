# trace·ca web

Single-page Next.js 14 frontend for the trace-ca agent API.

## Dev

```bash
cp .env.local.example .env.local   # points at http://localhost:8000 by default
npm install
npm run dev                         # http://localhost:3000
```

## Build / start

```bash
npm run build
npm run start
```

## Env

| Variable              | Purpose                                                                 |
| --------------------- | ----------------------------------------------------------------------- |
| `NEXT_PUBLIC_API_URL` | Base URL of the FastAPI agent. The browser hits `$NEXT_PUBLIC_API_URL/ask`. |

## Notes

- App Router, TypeScript, Tailwind. No external state library — `useState`.
- Fonts: Instrument Serif (display), IBM Plex Sans (body), IBM Plex Mono (code),
  all loaded via `next/font/google`.
- `⌘ + Return` submits the query.
