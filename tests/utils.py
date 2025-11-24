import os

import pymssql
import singer

import tap_mssql
import tap_mssql.sync_strategies.common as common
from tap_mssql.connection import MSSQLConnection

DB_NAME = "test_db"
SCHEMA_NAME = "dbo"


def get_db_config(use_env_db_name=False, use_schema_name=False):
    config = {}
    config["user"] = os.environ.get("TAP_MSSQL_USER", "SA")
    config["password"] = os.environ.get("TAP_MSSQL_PASSWORD", "testDatabase1")
    config["host"] = os.environ.get("TAP_MSSQL_HOST", "localhost")
    config["database"] = DB_NAME
    config["charset"] = "utf8"
    config["port"] = int(os.environ.get("TAP_MSSQL_PORT", 1433))
    config["tds_version"] = os.environ.get("TAP_MSSQL_TDS_VERSION", "7.3")

    if use_env_db_name:
        config["database"] = os.environ.get("TAP_MSSQL_DATABASE")
    elif use_schema_name:
        config["database"] = SCHEMA_NAME
    return config


def get_test_connection():
    db_config = get_db_config(use_env_db_name=True)

    # MSSQL Database must be in autocommit mode to Create a Database
    db_config["autocommit"] = True

    con = pymssql.connect(**db_config)

    try:
        with con.cursor() as cur:
            try:
                cur.execute("DROP DATABASE {}".format(DB_NAME))
            except:
                pass
            cur.execute("CREATE DATABASE {}".format(DB_NAME))
    finally:
        con.close()
    mssql_conn = MSSQLConnection(get_db_config())
    mssql_conn.autocommit_mode = True

    return mssql_conn


def discover_catalog(connection, config):
    catalog = {}
    config = get_db_config()
    catalog = tap_mssql.discover_catalog(connection, config)
    streams = []

    for stream in catalog.streams:
        database_name = common.get_database_name(stream)
        if database_name == SCHEMA_NAME:
            streams.append(stream)

    catalog.streams = streams
    return catalog


def set_replication_method_and_key(stream, r_method, r_key):
    new_md = singer.metadata.to_map(stream.metadata)
    old_md = new_md.get(())
    if r_method:
        old_md.update({"replication-method": r_method})

    if r_key:
        old_md.update({"replication-key": r_key})

    stream.metadata = singer.metadata.to_list(new_md)
    return stream


def set_replication_method_and_key_and_header_table_config(stream, r_method, r_key, h_replication_multi, h_replication, h_rep_table, h_rep_id, h_ch_rep_id):
    new_md = singer.metadata.to_map(stream.metadata)
    old_md = new_md.get(())
    if r_method:
        old_md.update({"replication-method": r_method})

    if r_key:
        old_md.update({"replication-key": r_key})
    if h_replication_multi:
        old_md.update({"header-multi-column-replication-key": h_replication_multi})
    if h_replication:
        old_md.update({"header-table-replication": h_replication})
    if h_rep_table:
        old_md.update({"header-table-replication-table": h_rep_table})
    if h_rep_id:
        old_md.update({"header-table-replication_id": h_rep_id})
    if True:
        old_md.update({"header-table-child-replication_id": h_ch_rep_id})

    stream.metadata = singer.metadata.to_list(new_md)
    return stream
