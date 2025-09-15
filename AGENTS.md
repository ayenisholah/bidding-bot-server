# Repository Guidelines

## Project Structure & Module Organization
- `src/` — TypeScript source. Key areas: `marketplace/{opensea,blur,magiceden}`, `functions/` (business logic, schedulers), `models/` (Mongoose), `utils/` (Redis, queues, crypto, logging), `types/`.
- `scripts/` and `src/scripts/` — operational and diagnostic helpers.
- Build output in `dist/` (via `tsc`).
- Env files: `.env`, `.env.example` (never commit secrets).

## Build, Test, and Development Commands
- `npm install` — install dependencies.
- `npm run dev` — start local stack (Docker in `compose.dev.yaml`) and run server with nodemon.
- `npm run build` — compile TypeScript to `dist/`.
- `npm start` — run compiled server (`dist/src/index.js`).
- `npm run lint` / `npm run lint:fix` — ESLint check/fix for `src/**/*.{ts,tsx}`.
- Docker helpers: `docker compose -f compose.dev.yaml up -d`, `npm run dc:down`.
- Targeted tests/scripts (no Jest): e.g., `npm run test:opensea-loops`, `npm run test:verify-fix`, or `ts-node src/scripts/fix-stuck-tasks.ts`.

## Coding Style & Naming Conventions
- Language: TypeScript (Node 20). Indentation: 2 spaces; use semicolons and double quotes.
- Filenames: `kebab-case.ts` for modules; PascalCase for types/interfaces.
- Prefer named exports; group marketplace-specific logic under `src/marketplace/<name>/`.
- Keep side effects in `index.ts` minimal; isolate utilities in `src/utils/`.
- Run `npm run lint` before pushing. Prettier is not configured; follow existing style.

## Testing Guidelines
- Tests are script-based. Place quick checks as `test-*.{ts,js}` at repo root or under `src/scripts/`.
- Run with `ts-node <file>` or existing npm scripts. Example: `ts-node test-opensea-fix.ts`.
- Ensure required env vars from `.env.example` are set; avoid hitting production services.

## Commit & Pull Request Guidelines
- Use concise, imperative commits. Recommended style: `feat(utils): add blur rate limiter` or `fix(opensea): handle 404 retries`.
- PRs must include: clear description, rationale, testing steps/commands, relevant logs or screenshots, and updated docs (.env/example, README) when applicable.
- Changes to queues, schedulers, or `src/index.ts` should call out migration/backfill steps and rollout/rollback plans.

## Security & Config Tips
- Never commit `.env` or secrets; update `.env.example` when adding variables.
- Be cautious with destructive commands (e.g., queue clears, `dc:destroy`) and document intended impact.
- Validate Redis/Mongo connectivity locally via Docker before debugging application code.

