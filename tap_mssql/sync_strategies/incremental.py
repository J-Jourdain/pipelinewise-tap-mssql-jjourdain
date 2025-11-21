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

    # Added below for multi-column incremental replication
    if isinstance(replication_key_metadata, list) and len(replication_key_metadata) > 1 and not multi_column_replication and not header_table_replication:  # Catch multiple replication keys passed, but no multi flag.
        LOGGER.warning(
            "multi-column-replication-key is False, but more than one replication-key was listed. Attempting multi column replication, setting multi-column-replication-key=True"    
        )
        multi_column_replication = True

    if multi_column_replication and not header_table_replication:
        replication_key_metadata, replication_key_multi_column, columns, catalog_entry = common.set_up_multi_column_replication(replication_key_metadata, catalog_entry, columns.copy())        

    header_table_replication_key_value = None

    if header_table_replication:
        header_table_replication_table = stream_metadata.get("header-table-replication-table", False)
        header_table_replication_id = stream_metadata.get("header-table-replication_id", None)
        header_multi_column_replication = stream_metadata.get("header-multi-column-replication-key", None)
        header_catalog_entry = next((copy.deepcopy(stream) for stream in non_cdc_catalog.streams if stream.table == header_table_replication_table ), None)
        header_table_replication_key_value = singer.get_bookmark(
            state, header_catalog_entry.tap_stream_id, "replication_key_value"
        )

        if header_catalog_entry is None:
            msg = "Tried get header table stream from database catalog, but no matching stream could be found for Header table: '{}'".format(header_table_replication_table)
            LOGGER.error(msg)
            raise Exception(
                msg
            )
        elif header_multi_column_replication:
            replication_key_metadata, replication_key_multi_column, header_columns, header_catalog_entry = common.set_up_multi_column_replication(replication_key_metadata, header_catalog_entry, [header_table_replication_id])
        header_table_select_sql = common.generate_select_sql(header_catalog_entry, header_columns, header_multi_column_replication, header_table_replication)

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
                    select_sql += """ WHERE CAST("{}" AS BIGINT) >= 
                    convert(bigint, convert (varbinary(8), '0x{}', 1))
                    ORDER BY "{}" ASC""".format(
                        replication_key_metadata, replication_key_value, replication_key_metadata
                    )
                elif header_table_replication and header_multi_column_replication:
                    select_sql += ' WHERE {} IN ({} WHERE (SELECT MAX(val) FROM (VALUES {}) AS t(val)) >= %(replication_key_value)s)'.format(
                        header_table_replication_id,
                        header_table_select_sql,
                        ", ".join([f"(ISNULL(\"{col}\", '1900-01-01'))" for col in replication_key_multi_column])
                    )
                elif header_table_replication:
                    select_sql += ' WHERE {} IN ({} WHERE "{}" >= %(replication_key_value)s) ORDER BY {}'.format(
                        header_table_replication_id,
                        header_table_select_sql,
                        replication_key_metadata,
                        header_table_replication_id
                    )
                elif multi_column_replication:
                    select_sql += ' WHERE (SELECT MAX(val) FROM (VALUES {}) AS t(val)) >= %(replication_key_value)s ORDER BY (SELECT MAX(val) FROM (VALUES {}) AS t(val)) ASC'.format(
                        ", ".join([f"(ISNULL(\"{col}\", '1900-01-01'))" for col in replication_key_multi_column]),
                        ", ".join([f"(ISNULL(\"{col}\", '1900-01-01'))" for col in replication_key_multi_column])
                    )
                else:
                    select_sql += ' WHERE "{}" >= %(replication_key_value)s ORDER BY "{}" ASC'.format(
                        replication_key_metadata, replication_key_metadata
                    )

                params["replication_key_value"] = replication_key_value
            elif replication_key_metadata is not None: # No state stored
                if multi_column_replication: # multi-column incremental replication initial run
                    select_sql += ' ORDER BY (SELECT MAX(val) FROM (VALUES {}) AS t(val)) ASC'.format(
                        ", ".join([f"(ISNULL(\"{col}\", '1900-01-01'))" for col in replication_key_multi_column])
                    )
                elif not header_table_replication:
                    select_sql += ' ORDER BY "{}" ASC'.format(replication_key_metadata)
                
 
            common.sync_query(
                cur, catalog_entry, state, select_sql, columns, stream_version, params, config, multi_column_replication, header_table_replication, header_table_replication_key_value
            )
