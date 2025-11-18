#!/usr/bin/env python3
# pylint: disable=duplicate-code

import pendulum
import singer
from datetime import datetime
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
    # {(): {'selected-by-default': False, 'database-name': 'dbo', 'is-view': False, 'selected': True, 'replication-method': 'INCREMENTAL', 'replication-key': 'InsertionTime', 'multi-column-replication-key': "CASE WHEN ISNULL(InsertionTime, '1900-01-01') >= ISNULL(ResultsRptStatusChngDateTime, '1900-01-01') THEN ISNULL(InsertionTime, ResultsRptStatusChngDateTime) ELSE ISNULL(ResultsRptStatusChngDateTime, InsertionTime) END", 'table-key-properties': []}, ('properties', 'ReportDBID'): {'selected-by-default': True, 'sql-datatype': 'int'}, ('properties', 'PatientID'): {'selected-by-default': True, 'sql-datatype': 'int'}, ('properties', 'InsertionTime'): {'selected-by-default': True, 'sql-datatype': 'datetime'}, ('properties', 'ResultsRptStatusChngDateTime'): {'selected-by-default': True, 'sql-datatype': 'datetime'}}
    stream_metadata = catalog_metadata.get((), {})
    # {'selected-by-default': False, 'database-name': 'dbo', 'is-view': False, 'selected': True, 'replication-method': 'INCREMENTAL', 'replication-key': 'InsertionTime', 'multi-column-replication-key': "CASE WHEN ISNULL(InsertionTime, '1900-01-01') >= ISNULL(ResultsRptStatusChngDateTime, '1900-01-01') THEN ISNULL(InsertionTime, ResultsRptStatusChngDateTime) ELSE ISNULL(ResultsRptStatusChngDateTime, InsertionTime) END", 'table-key-properties': []}
    replication_key_metadata = stream_metadata.get("replication-key")
    # InsertionTime
    replication_key_state = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "replication_key"
    )

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

    singer.write_message(activate_version_message)
    LOGGER.info("Beginning SQL")
    with connect_with_backoff(mssql_conn) as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns)
            params = {}

            if replication_key_value is not None:
                if catalog_entry.schema.properties[replication_key_metadata].format == "date-time":
                    replication_key_value = datetime.fromtimestamp(
                        pendulum.parse(replication_key_value).timestamp()
                    )
                # Handle timestamp incremental (timestamp)
                if catalog_entry.schema.properties[replication_key_metadata].format == 'rowversion':
                    select_sql += """ WHERE CAST("{}" AS BIGINT) >= 
                    convert(bigint, convert (varbinary(8), '0x{}', 1))
                    ORDER BY "{}" ASC""".format(
                        replication_key_metadata, replication_key_value, replication_key_metadata
                    )
                    
                else:
                    select_sql += ' WHERE "{}" >= %(replication_key_value)s ORDER BY "{}" ASC'.format(
                        replication_key_metadata, replication_key_metadata
                    )


                params["replication_key_value"] = replication_key_value
            elif replication_key_metadata is not None:
                #select_sql += ' ORDER BY "{}" ASC'.format(replication_key_metadata)
                replication_key_metadata_multi = ['InsertionTime', 'ResultsRptStatusChngDateTime']
                LOGGER.info(' ORDER BY (SELECT MAX(val) FROM (VALUES {}) AS t(val)) ASC'.format(", ".join([f"(ISNULL({col}, '1900-01-01'))" for col in replication_key_metadata_multi])))
                select_sql += ' ORDER BY (SELECT MAX(val) FROM (VALUES {}) AS t(val)) ASC'.format(", ".join([f"(ISNULL({col}, '1900-01-01'))" for col in replication_key_metadata_multi]))
                replication_key_sql_data_type = catalog_entry.schema.properties[replication_key_metadata].additionalProperties['sql_data_type']
                replication_key_format = catalog_entry.schema.properties[replication_key_metadata].additionalProperties['sql_data_type']
                catalog_entry.schema.properties["MultiReplicationKeyColumn"] = Schema(inclusion='automatic', additionalProperties=replication_key_sql_data_type, format=replication_key_format)
                columns.append('MultiReplicationKeyColumn')
 
            common.sync_query(
                cur, catalog_entry, state, select_sql, columns, stream_version, params, config
            )
