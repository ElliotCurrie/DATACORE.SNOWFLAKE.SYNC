"""
Environment-backed configuration.

Required secrets are read with os.environ[...] so the script fails fast if a
mandatory value is missing. Optional operational settings use os.getenv(...)
with safe defaults for the current Datacore/Snowflake deployment.
"""

import os

# Maximum number of source rows fetched and processed in a single batch.
# This can be reduced if the VM, network or target database struggles with
# larger batches, without requiring a code change.
BATCH_SIZE = int(
    os.getenv("DATACORE_SNOWFLAKE_SYNC_BATCH_SIZE", "50000")
)

# Optional safeguard against historic Fivetran re-syncs being treated as
# current source updates. Disabled by default so rebuilds and unconfigured
# deployments retain the existing behaviour and ingest all eligible rows.
APPLY_SYNCHREC_THRESHOLD = (
    os.getenv("DATACORE_SNOWFLAKE_SYNC_APPLY_SYNCHREC_THRESHOLD", "false")
    .strip()
    .lower()
    == "true"
)

# Maximum permitted delay between the record being synchronised with Reapit
# (SYNCHREC) and Fivetran loading it into Snowflake (_FIVETRAN_SYNCED).
# Rows beyond this threshold are excluded only when the safeguard is enabled
# and the source table contains a SYNCHREC column.
SYNCHREC_THRESHOLD_DAYS = int(
    os.getenv("DATACORE_SNOWFLAKE_SYNC_SYNCHREC_THRESHOLD_DAYS", "30")
)



# SQL Server / Azure SQL target database.
# Credentials should be supplied as machine/user environment variables on the
# VM or hosting environment; they should not be committed into this file.
datacore_config = {
    "driver": os.getenv("DATACORE_DRIVER", "ODBC Driver 18 for SQL Server"),
    "server": os.environ["DATACORE_SERVER"],
    "database": os.environ["DATACORE_DATABASE"],
    "uid": os.environ["DATACORE_UID"],
    "password": os.environ["DATACORE_PASSWORD"],
    "encrypt": os.getenv("DATACORE_ENCRYPT", "yes"),
    "trust_server_certificate": os.getenv("DATACORE_TRUST_SERVER_CERTIFICATE", "yes"),
    "connection_timeout": int(os.getenv("DATACORE_CONNECTION_TIMEOUT", "30")),
}

# Core Reapit Snowflake source. Defaults point at the current raw database and
# schema, but can be overridden for testing or future environment changes.
core_snowflake_config = {
    "user": os.environ["CORE_SNOWFLAKE_USER"],
    "password": os.environ["CORE_SNOWFLAKE_PASSWORD"],
    "account": os.environ["CORE_SNOWFLAKE_ACCOUNT"],
    "warehouse": os.getenv("CORE_SNOWFLAKE_WAREHOUSE", "STANDARD_WH"),
    "database": os.getenv("CORE_SNOWFLAKE_DATABASE", "REAPIT_RAW"),
    "schema": os.getenv("CORE_SNOWFLAKE_SCHEMA", "AURORA_ROM_RPS_ROM"),
    "login_timeout": int(os.getenv("CORE_SNOWFLAKE_LOGIN_TIMEOUT", "30")),
    "network_timeout": int(os.getenv("CORE_SNOWFLAKE_NETWORK_TIMEOUT", "60")),
}

# SA Reapit Snowflake source. Kept separate from Core because credentials and
# source schema differ, even though the sync engine is the same.
sa_snowflake_config = {
    "user": os.environ["SA_SNOWFLAKE_USER"],
    "password": os.environ["SA_SNOWFLAKE_PASSWORD"],
    "account": os.environ["SA_SNOWFLAKE_ACCOUNT"],
    "warehouse": os.getenv("SA_SNOWFLAKE_WAREHOUSE", "STANDARD_WH"),
    "database": os.getenv("SA_SNOWFLAKE_DATABASE", "REAPIT_RAW"),
    "schema": os.getenv("SA_SNOWFLAKE_SCHEMA", "AURORA_TWN_RPS_TWN"),
    "login_timeout": int(os.getenv("SA_SNOWFLAKE_LOGIN_TIMEOUT", "30")),
    "network_timeout": int(os.getenv("SA_SNOWFLAKE_NETWORK_TIMEOUT", "60")),
}
