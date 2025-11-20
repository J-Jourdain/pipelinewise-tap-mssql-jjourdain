#!/usr/bin/env python3
# pylint: disable=duplicate-code

import pendulum
import singer
from datetime import datetime, timezone
from singer import metadata
from singer.schema import Schema

import tap_mssql.sync_strategies.common as common
from tap_mssql.connection import MSSQLConnection, connect_with_backoff

LOGGER = singer.get_logger()

BOOKMARK_KEYS = {"replication_key", "replication_key_value", "version"}


def sync_table(mssql_conn, config, catalog_entry, state, columns):
    mssql_conn = MSSQLConnection(config)
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    
    stream_metadata = catalog_metadata.get((), {})
    
    replication_key_metadata = stream_metadata.get("replication-key")
    # InsertionTime
    replication_key_state = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "replication_key"
    )

    multi_column_replication = stream_metadata.get("multi-column-replication-key", False)

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

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream, version=stream_version
    )

    # Added below for multi-column incremental replication
    if isinstance(replication_key_metadata, list) and len(replication_key_metadata) > 1:  # Catch multiple replication keys passed, but no multi flag.
        LOGGER.warning(
            "multi-column-replication-key is False, but more than one replication-key was listed. Attempting multi column replication, setting multi-column-replication-key=True"    
        )
        multi_column_replication = True
    if multi_column_replication:
        replication_key_multi_column = replication_key_metadata
        data_types_of_replication_keys = [
            catalog_entry.schema.properties[col].additionalProperties.get('sql_data_type', 'No sql data type')
            if getattr(catalog_entry.schema.properties[col], 'additionalProperties', None)
            else 'No sql data type'
            for col in replication_key_metadata
        ]
        formats_of_replication_keys = [catalog_entry.schema.properties[col].format for col in replication_key_metadata]
        types_of_replication_keys = [catalog_entry.schema.properties[col].type for col in replication_key_metadata]
        if len(set(data_types_of_replication_keys)) == 1 and len(set(formats_of_replication_keys)) == 1 and all(lst == types_of_replication_keys[0] for lst in types_of_replication_keys):
            # Multiple replication key columns have been provided. All are of the same data type, so can continue. Adding manufactured column into catalog and column selection list:
            catalog_entry.schema.properties["MultiReplicationKeyColumn"] = Schema(
                inclusion='automatic',
                additionalProperties={'sql_data_type': data_types_of_replication_keys[0], 'replication_keys':replication_key_metadata},
                format=formats_of_replication_keys[0],
                type=types_of_replication_keys[0],
            )
            columns.append('MultiReplicationKeyColumn')
            replication_key_metadata = 'MultiReplicationKeyColumn'
        else: # Multiple replication key columns have been provided, but there is a difference in details for each column.
            msg = "Data types, formats, or types of specified replication key columns are not all the same Data types: '{}', formats: '{}', types: '{}".format(
                "', '".join(data_types_of_replication_keys),
                "', '".join(f if f is not None else 'None' for f in formats_of_replication_keys),
                "', '".join(str(t) if isinstance(t, list) else 'Not a list' for t in types_of_replication_keys))
            LOGGER.error(msg)
            raise Exception(
                msg
            )

    singer.write_message(activate_version_message)
    LOGGER.info("Beginning SQL")
    with connect_with_backoff(mssql_conn) as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns.copy())
            params = {}

            if replication_key_value is not None:
                if catalog_entry.schema.properties[replication_key_metadata].format == "date-time":
                    replication_key_value = datetime.fromtimestamp(
                        pendulum.parse(replication_key_value).timestamp(), tz=timezone.utc
                    )
                # Handle timestamp incremental (timestamp)
                if catalog_entry.schema.properties[replication_key_metadata].format == 'rowversion':
                    select_sql += """ WHERE CAST("{}" AS BIGINT) >= 
                    convert(bigint, convert (varbinary(8), '0x{}', 1))
                    ORDER BY "{}" ASC""".format(
                        replication_key_metadata, replication_key_value, replication_key_metadata
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
            elif replication_key_metadata is not None and multi_column_replication:
                select_sql += ' ORDER BY (SELECT MAX(val) FROM (VALUES {}) AS t(val)) ASC'.format(
                    ", ".join([f"(ISNULL(\"{col}\", '1900-01-01'))" for col in replication_key_multi_column])
                )
            elif replication_key_metadata is not None:
                select_sql += ' ORDER BY "{}" ASC'.format(replication_key_metadata)
                
 
            common.sync_query(
                cur, catalog_entry, state, select_sql, columns, stream_version, params, config, multi_column_replication
            )
