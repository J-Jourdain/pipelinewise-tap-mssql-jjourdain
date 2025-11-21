#!/usr/bin/env python3
# pylint: disable=duplicate-code

import pendulum
import singer
from datetime import datetime, timezone
import copy
from singer import metadata


import tap_mssql.sync_strategies.common as common
from tap_mssql.connection import MSSQLConnection, connect_with_backoff

LOGGER = singer.get_logger()

BOOKMARK_KEYS = {"replication_key", "replication_key_value", "version"}


def sync_table(mssql_conn, config, catalog_entry, non_cdc_catalog, state, columns):
    mssql_conn = MSSQLConnection(config)
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})
    
    replication_key_metadata = stream_metadata.get("replication-key")
    # InsertionTime
    replication_key_state = singer.get_bookmark(state, catalog_entry.tap_stream_id, "replication_key")

    multi_column_replication = stream_metadata.get("multi-column-replication-key", False)
    header_table_replication = stream_metadata.get("header-table-replication", False)

    replication_key_value = None

    if replication_key_metadata == replication_key_state:
        replication_key_value = singer.get_bookmark(
            state, catalog_entry.tap_stream_id, "replication_key_value"
        )
    else:
        state = singer.write_bookmark(
            state, catalog_entry.tap_stream_id, "replication_key", replication_key_metadata
        )
        state = singer.clear_bookmark(state, catalog_entry.tap_stream_id, "replication_key_value")

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state, catalog_entry.tap_stream_id, "version", stream_version)
    singer.write_message(singer.ActivateVersionMessage(stream=catalog_entry.stream, version=stream_version))

    # --- Replication mode adjustments ---
    multi_column_replication, replication_key_multi_column, replication_key_metadata, columns, catalog_entry = handle_multi_column(
        replication_key_metadata, catalog_entry, columns, multi_column_replication, header_table_replication
    )

    header_table_replication_id, header_table_replication_key_value, header_catalog_entry, header_multi_column_replication, header_table_select_sql, replication_key_metadata  = \
        handle_header_table_replication(stream_metadata, non_cdc_catalog, state, replication_key_metadata, header_table_replication)

    LOGGER.info("Beginning SQL")
    with connect_with_backoff(mssql_conn) as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns.copy(), multi_column_replication, header_table_replication)
            params = {}

            if replication_key_value is not None:  # Resuming from state
                if header_table_replication and header_catalog_entry.schema.properties[replication_key_metadata].format == "date-time":
                    replication_key_value = datetime.fromtimestamp(
                        pendulum.parse(replication_key_value).timestamp(), tz=timezone.utc
                    )
                elif catalog_entry.schema.properties[replication_key_metadata].format == "date-time":
                    replication_key_value = datetime.fromtimestamp(
                        pendulum.parse(replication_key_value).timestamp(), tz=timezone.utc
                    )
                # Handle timestamp incremental (timestamp)
                if not header_table_replication and catalog_entry.schema.properties[replication_key_metadata].format == 'rowversion':
                    select_sql += build_rowversion_where_sql(replication_key_metadata, replication_key_value)
                elif header_table_replication:
                    select_sql += build_header_table_where_sql(header_table_replication_id, header_table_select_sql, replication_key_metadata, header_multi_column_replication, replication_key_multi_column)
                elif multi_column_replication:
                    select_sql += build_multi_column_where_sql(replication_key_multi_column)
                else:
                    select_sql += build_default_where_sql(replication_key_metadata)

                params["replication_key_value"] = replication_key_value
            elif replication_key_metadata is not None: # No state stored
                build_order_by_sql(replication_key_metadata, multi_column_replication, header_table_replication, replication_key_multi_column)
                
            common.sync_query(
                cur, catalog_entry, state, select_sql, columns, stream_version, params, config, multi_column_replication, header_table_replication, header_table_replication_key_value
            )


def handle_multi_column(replication_key_metadata, catalog_entry, columns, multi_column_replication, header_table_replication):
    """Enable multi-column replication if needed."""
    if isinstance(replication_key_metadata, list) and len(replication_key_metadata) > 1 \
       and not multi_column_replication and not header_table_replication:
        LOGGER.warning("Multiple replication keys detected. Enabling multi-column replication.")
        multi_column_replication = True

    replication_key_multi_column = replication_key_metadata
    if multi_column_replication and not header_table_replication:
        replication_key_metadata, replication_key_multi_column, columns, catalog_entry = \
            common.set_up_multi_column_replication(replication_key_metadata, catalog_entry, columns.copy())

    return multi_column_replication, replication_key_multi_column, replication_key_metadata, columns, catalog_entry


def handle_header_table_replication(stream_metadata, non_cdc_catalog, state, replication_key_metadata, header_table_replication):
    """Handle header-table replication setup."""
    if not header_table_replication:
        return None, None, None, None, None, replication_key_metadata, 

    header_table_replication_table = stream_metadata.get("header-table-replication-table")
    header_table_replication_id = stream_metadata.get("header-table-replication_id")
    header_multi_column_replication = stream_metadata.get("header-multi-column-replication-key")

    header_catalog_entry = next(
        (copy.deepcopy(stream) for stream in non_cdc_catalog.streams if stream.table == header_table_replication_table),
        None
    )
    if header_catalog_entry is None:
        msg = f"No matching stream found for Header table: '{header_table_replication_table}'"
        LOGGER.error(msg)
        raise Exception(msg)

    header_table_replication_key_value = singer.get_bookmark(
        state, header_catalog_entry.tap_stream_id, "replication_key_value"
    )

    header_columns = [header_table_replication_id]
    if header_multi_column_replication:
        replication_key_metadata, replication_key_multi_column, header_columns, header_catalog_entry = \
            common.set_up_multi_column_replication(replication_key_metadata, header_catalog_entry, header_columns)
        
    header_table_select_sql = common.generate_select_sql(header_catalog_entry, header_columns, header_multi_column_replication, header_table_replication)

    return header_table_replication_id, header_table_replication_key_value, header_catalog_entry, header_multi_column_replication, header_table_select_sql, replication_key_metadata


def format_multi_column(replication_key_multi_column):
    """Format multi-column replication keys for SQL."""
    return ", ".join([f"(ISNULL(\"{col}\", '1900-01-01'))" for col in replication_key_multi_column])


def build_rowversion_where_sql(replication_key_metadata, replication_key_value):
    return """ WHERE CAST("{}" AS BIGINT) >= 
    convert(bigint, convert (varbinary(8), '0x{}', 1))
    ORDER BY "{}" ASC""".format(
        replication_key_metadata, replication_key_value, replication_key_metadata
    )


def build_header_table_where_sql(header_table_replication_id, header_table_select_sql, replication_key_metadata, multi_column, replication_key_multi_column):
    if multi_column:
        return ' WHERE {} IN ({} WHERE (SELECT MAX(val) FROM (VALUES {}) AS t(val)) >= %(replication_key_value)s)'.format(
            header_table_replication_id,
            header_table_select_sql,
            format_multi_column(replication_key_multi_column)
        )
    return ' WHERE {} IN ({} WHERE "{}" >= %(replication_key_value)s) ORDER BY {}'.format(
        header_table_replication_id,
        header_table_select_sql,
        replication_key_metadata,
        header_table_replication_id
    )


def build_multi_column_where_sql(replication_key_multi_column):
    formatted = format_multi_column(replication_key_multi_column)
    return ' WHERE (SELECT MAX(val) FROM (VALUES {}) AS t(val)) >= %(replication_key_value)s ORDER BY (SELECT MAX(val) FROM (VALUES {}) AS t(val)) ASC'.format(
        formatted, formatted
    )


def build_default_where_sql(replication_key_metadata):
    return ' WHERE "{}" >= %(replication_key_value)s ORDER BY "{}" ASC'.format(
        replication_key_metadata, replication_key_metadata
    )


def build_order_by_sql(replication_key_metadata, multi_column, header_table_replication, replication_key_multi_column):
    if multi_column: # multi-column incremental replication initial run
        return ' ORDER BY (SELECT MAX(val) FROM (VALUES {}) AS t(val)) ASC'.format(
            format_multi_column(replication_key_multi_column)
        )
    elif not header_table_replication:
        return ' ORDER BY "{}" ASC'.format(replication_key_metadata)    
