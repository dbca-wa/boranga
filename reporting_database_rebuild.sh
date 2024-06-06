#!/bin/bash

echo "Running database collection and merge into new database for reporting.";

if [ $TEMPORARY_LEDGER_DATABASE ==  'ledger_prod' ]; then
        echo "ERROR: ledger_prod can not be a TEMPORARY Database";
        exit;
fi

if [ $TEMPORARY_LEDGER_DATABASE ==  'boranga_prod' ]; then
        echo "ERROR: boranga_prod can not be a TEMPORARY Database";
        exit;
fi

echo "Dump Core Ledger Production Tables";
PGPASSWORD="$PRODUCTION_LEDGER_PASSWORD" pg_dump -t 'accounts_*' --file /dbdumps/ledger_core_prod.sql --format=custom --host $PRODUCTION_LEDGER_HOST --dbname $PRODUCTION_LEDGER_DATABASE --username $PRODUCTION_LEDGER_USERNAME

echo "Dump Core Boranga Production Tables";
PGPASSWORD="$PRODUCTION_LEDGER_PASSWORD" pg_dump -t 'boranga_*' --file /dbdumps/boranga_core_prod.sql --format=custom --host $PRODUCTION_LEDGER_HOST --dbname $PRODUCTION_LEDGER_DATABASE --username $PRODUCTION_LEDGER_USERNAME

# DROP All TABLES IN DAILY DB
for I in $(psql "host=$TEMPORARY_LEDGER_HOST port=5432 dbname=$TEMPORARY_LEDGER_DATABASE user=$TEMPORARY_LEDGER_USERNAME password=$TEMPORARY_LEDGER_PASSWORD sslmode=require" -c "SELECT tablename FROM pg_tables where tablename not like 'pg\_%' and tablename not like 'sql\_%';" -t);
  do
  echo " drop table $I CASCADE; ";
  psql "host=$TEMPORARY_LEDGER_HOST port=5432 dbname=$TEMPORARY_LEDGER_DATABASE user=$TEMPORARY_LEDGER_USERNAME password=$TEMPORARY_LEDGER_PASSWORD sslmode=require" -c "drop table $I CASCADE;" -t
done

# IMPORT LEDGER PROD DATABASE INTO REPORTING 
echo "Importing Ledger Core prod into reporting database";
PGSSLMODE=require PGPASSWORD="$TEMPORARY_LEDGER_PASSWORD"  pg_restore  --host=$TEMPORARY_LEDGER_HOST --dbname=$TEMPORARY_LEDGER_DATABASE --username=$TEMPORARY_LEDGER_USERNAME /dbdumps/ledger_core_prod.sql

# Boranga
echo "Importing Boranga prod into reporting database";
PGSSLMODE=require PGPASSWORD="$TEMPORARY_LEDGER_PASSWORD"  pg_restore  --host=$TEMPORARY_LEDGER_HOST --dbname=$TEMPORARY_LEDGER_DATABASE --username=$TEMPORARY_LEDGER_USERNAME /dbdumps/boranga_core_prod.sql

# drop all sequences
for I in $(psql "host=$TEMPORARY_LEDGER_HOST port=5432 dbname=$TEMPORARY_LEDGER_DATABASE user=$TEMPORARY_LEDGER_USERNAME password=$TEMPORARY_LEDGER_PASSWORD sslmode=require" -c "select relname from pg_class  where relkind = 'S' and relname like 'boranga\_%' ;" -t);
  do
  echo "DROP SEQUENCE IF EXISTS $I CASCADE; ";
  psql "host=$TEMPORARY_LEDGER_HOST port=5432 dbname=$TEMPORARY_LEDGER_DATABASE user=$TEMPORARY_LEDGER_USERNAME password=$TEMPORARY_LEDGER_PASSWORD sslmode=require" -c "DROP SEQUENCE IF EXISTS $I CASCADE; " -t
done

# GRANT SELECT to boranga_ro account
for I in $(psql "host=$TEMPORARY_LEDGER_HOST port=5432 dbname=$TEMPORARY_LEDGER_DATABASE user=$TEMPORARY_LEDGER_USERNAME password=$TEMPORARY_LEDGER_PASSWORD sslmode=require" -c "SELECT tablename FROM pg_tables where tablename not like 'pg\_%' and tablename not like 'sql\_%';" -t);
  do
  echo "GRANT SELECT ON $I TO boranga_ro;";
  psql "host=$TEMPORARY_LEDGER_HOST port=5432 dbname=$TEMPORARY_LEDGER_DATABASE user=$TEMPORARY_LEDGER_USERNAME password=$TEMPORARY_LEDGER_PASSWORD sslmode=require" -c "GRANT SELECT ON $I TO boranga_ro; " -t
done

rm /dbdumps/ledger_core_prod.sql
rm /dbdumps/boranga_core_prod.sql

