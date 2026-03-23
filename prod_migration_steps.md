## Step 1: Login to rks.dbca.wa.gov.au (and run below series of commands)

## Step 2: Create workloads boranga-prod & boranga-prod-cron

## Step 3: Create shared secrets for env and add localised workload envs

_Note: Set SENTRY_DSN_DISABLED (not SENTRY_DSN) for now — Sentry will be enabled once the application is stable to avoid
using up our events allocation (Step 20)_

## Step 4: Change image to ghcr.io/dbca-wa/boranga:XXXX.XX.XX.XX.XXXX

## Step 5: Setup ingress rules for https://boranga-internal.dbca.wa.gov.au/

_Note: External ingress (https://boranga.dbca.wa.gov.au/) will be configured at a later date when the system is opened to external users_

## Step 6: Migrate the auth and ledger api client apps

_Note: Steps 6–13 are run by opening a shell on the boranga-prod workload in the Rancher UI_

```
./manage.py migrate auth && ./manage.py migrate ledger_api_client
```

## Step 7: Apply the admin migration patch

```
patch venv/lib/python3.12/site-packages/django/contrib/admin/migrations/0001_initial.py 0001_intial.py.patch
```

_Note: The path to the virtual environment may vary on your local system_

## Step 8: Migrate the admin app

```
./manage.py migrate admin
```

## Step 9: Reverse the admin migration patch

```
patch -R venv/lib/python3.12/site-packages/django/contrib/admin/migrations/0001_initial.py 0001_intial.py.patch
```

## Step 10: Apply the reversion migration patch

```
patch venv/lib/python3.12/site-packages/reversion/migrations/0001_squashed_0004_auto_20160611_1202.py 0001_squashed_0004_auto_20160611_1202.py.patch
```

## Step 11: Migrate the reversion app

```
./manage.py migrate reversion

```

## Step 12: Reverse the reversion migration patch

```
patch -R venv/lib/python3.12/site-packages/reversion/migrations/0001_squashed_0004_auto_20160611_1202.py 0001_squashed_0004_auto_20160611_1202.py.patch
```

## Step 13: Run the rest of the migrations

```
./manage.py migrate

```

## Step 14: Wait 1 minute for health checks and container to load

## Step 15: Setup AppMonitor for health checks

## Step 16: Install all required fixtures (previously created based on data from the UAT environment)

1. Upload `boranga/fixtures/uat/uat_fixtures.json` to blob storage using Microsoft Azure Storage Explorer (drag and drop)
2. From the Rancher workload shell, copy the file into the container using the `AZCOPY_PATH` env var:

```
azcopy copy "${AZCOPY_PATH}/uat_fixtures.json" boranga/fixtures/uat/uat_fixtures.json
```

3. Load the fixtures:

```
./manage.py loaddata boranga/fixtures/uat/uat_fixtures.json
```

## Step 17: Populate the Proxy record

The proxy fixture is not imported automatically as it contains credentials. Manually create the Proxy record via the Django admin or shell using the credentials from the secrets store.

## Step 18: Import Taxonomies from NOMOS (Can take up to ~30 minutes)

Ensure NOMOS_BLOB_URL secret is set

./manage.py fetch_nomos_blob_data

## Step 19: Import the Cadastre Layer from KB (Can take up to ~12 minutes)

Make sure KB_CADASTRE_LAYER_URL, KB_AUTH_USER, KB_AUTH_PASS secrets are set

./manage.py import_cadastre_geojson --check-auth

./manage.py import_cadastre_geojson

## Step 20: Run full TPFL legacy data migration — outlined in boranga/components/data_migration/MIGRATION_ORDER.md (Can take up to 3 hours)

## Step 21: Once the web application is stable, enable sentry

Rename the secret SENTRY_DSN_DISABLED to SENTRY_DSN

Then verify Sentry is receiving events and reporting the correct environment by navigating to the following URL in a browser (also validates ingress end-to-end):

https://boranga-internal.dbca.wa.gov.au/api/sentry-debug/

Check the Sentry dashboard to confirm the error appears under the **prod** environment.
