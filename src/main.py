"""
Entry point for the Snowflake -> Datacore replication process.

The script runs the same sync engine against multiple Snowflake instances
(currently Core and SA) and writes into separate SQL Server schemas.

Each run:

1. checks database connectivity;
2. discovers new offices and assigns them to the backlog lane;
3. handles target/staging schema changes;
4. processes each configured table in batches;
5. records run/table status in ops sync log tables;
6. continues past table-level failures so one bad table does not block the rest.
"""

import utils

from clients import SqlServerClient, SnowflakeClient
from configs import (
    core_snowflake_config,
    datacore_config,
    sa_snowflake_config,
)


def main(datacore, snowflake, sync_schema_name):
    """
    Run one full sync cycle for a single Snowflake instance/schema pair.

    Table-level failures are collected and reported as completed_with_errors;
    only failures before/during orchestration are treated as run-level failure.
    """

    # -------------------------------------------------------------------------
    # 1. CONNECTION CHECKS
    # -------------------------------------------------------------------------

    utils.log(
        f"Checking SQL Server and Snowflake connections for {sync_schema_name}..."
    )

    utils.wait_for_all_connections(datacore, snowflake)

    # -------------------------------------------------------------------------
    # 2. OFFICE DISCOVERY
    # -------------------------------------------------------------------------

    utils.log(f"Checking for new offices in {sync_schema_name}...")

    new_offices = utils.discover_new_offices(
        snowflake=snowflake,
        datacore=datacore,
        schema_name=sync_schema_name,
    )

    if new_offices:
        utils.log(
            f"{sync_schema_name}: discovered {len(new_offices):,} new office(s): "
            f"{', '.join(new_offices)}"
        )
    else:
        utils.log(f"{sync_schema_name}: no new offices found.")

    # -------------------------------------------------------------------------
    # 3. SCHEMA HANDLING
    # -------------------------------------------------------------------------

    utils.log(f"Handling database schema changes for {sync_schema_name}...")

    schema_change_result = utils.handle_schema_changes(
        snowflake=snowflake,
        datacore=datacore,
        sync_schema_name=sync_schema_name,
        staging_schema_name="stg",
    )

    utils.log(
        f"Schema handling complete for {sync_schema_name}. "
        f"Target tables added: {len(schema_change_result['target_tables_added'])}. "
        f"Staging tables added: {len(schema_change_result['staging_tables_added'])}."
    )

    # -------------------------------------------------------------------------
    # 4. START SYNC RUN
    # -------------------------------------------------------------------------

    utils.log(f"Starting sync run for {sync_schema_name}...")

    sync_log_id = datacore.run_when_available(utils.start_sync_run)

    utils.log(
        f"Sync run started for {sync_schema_name}. "
        f"sync_log_id={sync_log_id}"
    )

    total_rows_processed = 0
    failed_tables = []

    try:
        # ---------------------------------------------------------------------
        # 5. LOAD TABLE CONFIGURATION
        # ---------------------------------------------------------------------

        table_config = datacore.run_when_available(
            utils.fetch_table_config,
            schema_name=sync_schema_name,
        )

        utils.log(
            f"Fetched {len(table_config):,} tables from ops.table_config "
            f"for schema '{sync_schema_name}'."
        )

        # ---------------------------------------------------------------------
        # 6. PROCESS TABLES
        # ---------------------------------------------------------------------

        # Tables are deliberately processed serially for simpler locking,
        # staging behaviour and failure recovery.
        for index, row in enumerate(table_config, start=1):
            schema_name = row["schema_name"]
            table_name = row["table_name"]
            full_table_name = f"{schema_name}.{table_name}"

            utils.log(
                f"Starting table {index:,}/{len(table_config):,}: "
                f"{full_table_name} "
                f"pk={row['primary_key']} "
                f"last_synced={row['last_synced']} "
                f"last_pk={row['last_pk']}"
            )

            try:
                rows_processed = utils.sync_table_until_empty(
                    datacore=datacore,
                    snowflake=snowflake,
                    sync_log_id=sync_log_id,
                    table_config_row=row,
                )

                total_rows_processed += rows_processed

                utils.log(
                    f"Finished table {full_table_name}. "
                    f"Rows processed this table: {rows_processed:,}. "
                    f"Run total: {total_rows_processed:,}."
                )

            except Exception as table_error:
                # A single failed table should not prevent unrelated tables
                # from continuing to replicate.
                failed_tables.append(full_table_name)

                utils.log(
                    f"Table failed: {full_table_name}. "
                    f"Error: {table_error}. "
                    f"Continuing with next table..."
                )

        # ---------------------------------------------------------------------
        # 7. COMPLETE SYNC RUN
        # ---------------------------------------------------------------------

        if failed_tables:
            error_summary = utils.build_failed_tables_error(
                failed_tables,
                max_length=1000,
            )

            datacore.run_when_available(
                utils.update_sync_run,
                sync_log_id=sync_log_id,
                status="completed_with_errors",
                error=error_summary,
            )

            utils.log(
                f"Sync run marked as completed_with_errors for {sync_schema_name}. "
                f"sync_log_id={sync_log_id}. "
                f"Failed tables: {len(set(failed_tables)):,}. "
                f"Rows processed: {total_rows_processed:,}. "
                f"{error_summary}"
            )

        else:
            datacore.run_when_available(
                utils.update_sync_run,
                sync_log_id=sync_log_id,
                status="completed",
                error=None,
            )

            utils.log(
                f"Sync run marked as completed for {sync_schema_name}. "
                f"sync_log_id={sync_log_id}. "
                f"Rows processed: {total_rows_processed:,}."
            )

        return total_rows_processed

    except Exception as e:
        # ---------------------------------------------------------------------
        # 8. CATASTROPHIC RUN FAILURE
        # ---------------------------------------------------------------------

        utils.log(
            f"Sync run failed catastrophically for {sync_schema_name}. "
            f"sync_log_id={sync_log_id}. Error: {e}"
        )

        datacore.run_when_available(
            utils.update_sync_run,
            sync_log_id=sync_log_id,
            status="failed",
            error=f"Run-level failure before table sync completed: {type(e).__name__}",
        )

        raise


if __name__ == "__main__":

    # -------------------------------------------------------------------------
    # CLIENT SETUP
    # -------------------------------------------------------------------------

    datacore = SqlServerClient(**datacore_config)

    snowflake_instances = [
        {
            "name": "core",
            "client": SnowflakeClient(**core_snowflake_config),
            "sync_schema_name": "reapit",
        },
        {
            "name": "sa",
            "client": SnowflakeClient(**sa_snowflake_config),
            "sync_schema_name": "reapit_sa",
        },
    ]

    # -------------------------------------------------------------------------
    # INSTANCE ORCHESTRATION
    # -------------------------------------------------------------------------

    for sf in snowflake_instances:
        try:
            utils.log(
                f"Starting Snowflake sync instance: "
                f"{sf['name']} -> {sf['sync_schema_name']}"
            )

            rows_processed = main(
                datacore=datacore,
                snowflake=sf["client"],
                sync_schema_name=sf["sync_schema_name"],
            )

            utils.log(
                f"Sync instance completed: "
                f"{sf['name']} -> {sf['sync_schema_name']}. "
                f"Rows processed: {rows_processed:,}"
            )

        except Exception as e:
            utils.log(
                f"Sync instance failed: "
                f"{sf['name']} -> {sf['sync_schema_name']}. "
                f"Error: {e}"
            )

    utils.log("All Snowflake instances processed.")
