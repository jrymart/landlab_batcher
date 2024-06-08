MODEL_PARAM_TABLE_SQL_START = """
CREATE TABLE model_run_params (
    run_param_id INTEGER PRIMARY KEY AUTOINCREMENT,
"""
MODEL_PARAM_TABLE_SQL_END = """
    model_run_id TEXT,
    model_batch_id TEXT
);
"""

MODEL_RUN_TABLE_SQL = """
CREATE TABLE model_run_metadata (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_run_id TEXT,
    model_batch_id TEXT,
    model_start_time REAL,
    model_end_time REAL
);
"""

from collections.abc import MutableMapping
import sqlite3

def _flatten_dict_gen(d, parent_key, sep):
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v

def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '.'):
    return dict(_flatten_dict_gen(d, parent_key, sep))

def python_type_to_sql_type(value):
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    else:
        return "TEXT"

def generate_model_param_table_sql(run_params):
    table_creation_sql = MODEL_PARAM_TABLE_SQL_START
    flat_run_params = flatten_dict(run_params, "model_param")
    for param, value in flat_run_params.items():
        param_type = python_type_to_sql_type(value)
        table_creation_sql += "%s %s," % (param, param_type)
    table_creation_sql += MODEL_PARAM_TABLE_SQL_END
    return table_creation_sql

def generate_model_run_db(db_path, params):
    sqliteConnection = sqlite3.connect(db_path)
    cursor = sqliteConnection.cursor()
    model_param_sql = generate_model_param_table_sql(params)
    cursor.execute(model_param_sql)
    cursor.execute(MODEL_RUN_TABLE_SQL)
    cursor.close()
