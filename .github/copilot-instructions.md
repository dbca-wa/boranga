## Boranga — AI assistant quickstart

This file contains concise, project-specific guidance to help an AI coding agent be productive immediately in the Boranga repo.

- Project type: Django (Python 3.x) monolith with a Vite + Vue 3 frontend.
- DB: Postgres / PostGIS. External dependency: ledger (separate repo/service).

Key places to read first

- `boranga/urls.py` — central routing. Note: most APIs are registered on a DRF router here (see `router.register(...)`) and many `re_path(r"^api/..."...)` endpoints are defined alongside the router.
- `boranga/components/*/api.py` — per-component DRF ViewSets and APIViews. Pattern: each component exposes a module named `api` and (sometimes) `views`.
- `boranga/views.py` — high-level page routing and public/internal view classes (e.g., `BorangaRoutingView`).
- `boranga/settings.py` — runtime flags (DEBUG, INSTALLED_APPS, SITE_PREFIX, etc.).
- `boranga/management/default_data_manager.py` — default data is loaded at startup (invoked from `urls.py` unless migrations are running).
- `frontend/boranga` — Vite + Vue frontend. npm scripts: `npm run dev` (dev server) and `npm run build` (prod build).

Important architecture notes (the "why")

- Components are organized by feature under `boranga/components/<feature>`; each component commonly exposes an `api.py` (DRF endpoints), `views.py` for Django views, and models under `boranga/models.py` or component-local models.
- The main `urls.py` composes a DRF router plus many hand-crafted `re_path` endpoints. When adding an API, prefer registering a ViewSet on the existing `router` (consistent names, many paginated variants exist: e.g. `species_paginated`).
- Ledger is an explicitly required external service. The repo includes `ledger_api_client.urls` in `urls.py` — Ledger must be running and accessible (see root `README.md` for required env vars).
- Startup side-effect: `DefaultDataManager()` is called from `urls.py` except when migrations are running. Follow the `are_migrations_running()` pattern to avoid loading defaults during `migrate`/`makemigrations`.

Developer workflows (commands discovered in the repo)

- Backend setup
  - Create virtualenv and install: `pip install -r requirements.txt`
  - Set environment vars as described in `README.md` (DATABASE_URL, LEDGER_API_URL, LEDGER_API_KEY, DEBUG, etc.).
  - Run migrations: `./manage.py migrate` (DefaultDataManager is skipped automatically during migrations).
  - Create superuser: `./manage.py createsuperuser`
  - Run tests: `./manage.py test` (or target tests in `boranga/tests.py`).
  - Run dev server: `./manage.py runserver <PORT>`
- Frontend
  - Install and build: `cd frontend/boranga && npm install && npm run build`
  - Dev server: `cd frontend/boranga && npm run dev` (the workspace includes an npm task `vite` configured to run dev with HOST=127.0.0.1 and PORT=5173).
- Assets
  - Collect static: `./manage.py collectstatic`

Project-specific conventions and patterns (do not assume defaults)

- API naming: many endpoints follow explicit naming conventions — e.g. `*_paginated` suffix for paginated ViewSets, `*_documents` for document endpoints, `*_referrals` for referral endpoints. Use these patterns when adding new endpoints.
- Component API placement: add endpoint logic in `boranga/components/<component>/api.py`. Import and register in `boranga/urls.py` rather than duplicating router creation.
- URL style: mix of `path()` and `re_path()` is used; `re_path` is common for API endpoints that need regex capture groups (see history endpoints in `urls.py`).
- Startup checks: take care around `DefaultDataManager()` side-effects. Follow the `are_migrations_running()` function in `boranga/urls.py` when making startup-time changes.

Integration points and external dependencies

- Ledger: mandatory. The project expects ledger DB and ledger API to exist. See root README: install/configure ledger first and set `LEDGER_API_URL` and `LEDGER_API_KEY`.
- PostGIS: ensure Postgres + PostGIS installed and reachable via `DATABASE_URL`.
- Nomos and other external APIs: check env vars in `README.md` (e.g., `NOMOS_URL`, `NOMOS_BLOB_URL`).

Examples (copyable patterns)

- Register a new ViewSet in `boranga/urls.py`:
  router.register(r"my_feature", my_feature_api.MyFeatureViewSet, "my_feature")
- Add a simple APIView-route next to existing API patterns:
  re_path(r"^api/my_lookup$", my_feature_api.MyLookupView.as_view(), name="my-lookup")
- Protect side-effects during migrations:
  if not are_migrations_running():
  MyStartupManager()

Where to look for tests & migrations

- Tests: `boranga/tests.py` and possible component-level tests under `boranga/components/*/tests*`.
- Migrations: `boranga/migrations/` and component-level migration files. See `migration_steps.md` for repo-specific migration guidance.

If you change APIs

- Update `router.register(...)` in `boranga/urls.py` and add corresponding `re_path` entries for non-ViewSet endpoints.
- Add or update API docs/comments close to the `api.py` implementation; follow existing naming/parameter conventions.

If anything is unclear or you'd like me to expand a section (examples for adding a ViewSet, running a specific test, or wiring frontend -> backend), tell me which area to expand and I'll iterate.

LEGACY DATA MIGRATION

This project includes legacy data migration components under `boranga/components/data_migration`.

That folder contains a README.md with specific instructions on how to use the data migration framework, including how to define adapters, schemas, and run migrations.

When working in that area, YOU MUST read `boranga/components/data_migration/README.md` first. It serves as the definitive guide for data migration architecture, including how to define adapters/schemas, which source files to use for each handler, and how to verify changes.
