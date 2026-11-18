## Step 1: Create a new database for Boranga

```
CREATE DATABASE boranga_dev;
CREATE USER boranga_dev WITH PASSWORD '<password>';
GRANT ALL PRIVILEGES ON DATABASE "boranga_dev" to boranga_dev;
\c boranga_dev
CREATE EXTENSION postgis;
GRANT ALL ON ALL TABLES IN SCHEMA public TO boranga_dev;
GRANT ALL ON SCHEMA public TO boranga_dev;
\q
```

## Step 2: Migrate the auth and ledger api client apps

```
./manage.py migrate auth && ./manage.py migrate ledger_api_client
```

## Step 2: Apply the admin migration patch

### Make sure to update this to match the path to your specific virtual environment and python version

```
patch /app/venv/lib/python3.15/site-packages/django/contrib/admin/migrations/0001_initial.py 0001_initial.py.patch
```

_Note: The path to the virtual environment may vary on your local system_

## Step 3: Migrate the admin app

```
./manage.py migrate admin
```

## Step 4: Reverse the admin migration patch

### Make sure to update this to match the path to your specific virtual environment and python version

```
patch -R /app/venv/lib/python3.15/site-packages/django/contrib/admin/migrations/0001_initial.py 0001_initial.py.patch
```

## Step 5: Apply the reversion migration patch

### Make sure to update this to match the path to your specific virtual environment and python version

```
patch /app/venv/lib/python3.15/site-packages/reversion/migrations/0001_squashed_0004_auto_20160611_1202.py 0001_squashed_0004_auto_20160611_1202.py.patch
```

## Step 6: Migrate the reversion app

```
./manage.py migrate reversion

```

## Step 7: Reverse the reversion migration patch

### Make sure to update this to match the path to your specific virtual environment and python version

```
patch -R /app/venv/lib/python3.15/site-packages/reversion/migrations/0001_squashed_0004_auto_20160611_1202.py 0001_squashed_0004_auto_20160611_1202.py.patch
```

## Step 8: Run the rest of the migrations

```
./manage.py migrate

```

## Step 9 Apply required fixtures

```

./manage.py loaddata \
    file_extension_whitelist_data.json \
    helptextentries.json \
	regions.json \
	districts.json \
	grouptypes.json \
	systememailgroups.json \
	submitter_category.json \
	landforms.json \
	observer_categories.json \
	observer_roles.json \
	bulk_import_schemas.json \
	bulk_import_schema_columns.json


```
