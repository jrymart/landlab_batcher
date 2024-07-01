import json
import re
import numpy as np
from copy import deepcopy
import sqlite3
from collections.abc import MutableMapping

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

PARAM_DIM_TABLE_SQL = """
CREATE TABLE model_param_dimension (
    param_name TEXT,
    python_type TEXT
);
                      """

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
        table_creation_sql += "\"%s\" %s," % (param, param_type)
    table_creation_sql += MODEL_PARAM_TABLE_SQL_END
    return table_creation_sql

def generate_model_param_dim_table_sql(run_params):
    flat_run_params = flatten_dict(run_params, "model_param")
    insertion_string = str([(key, str(type(value))) for key, value in flat_run_params.items()])[1:-1]
    insert_sql = "INSERT INTO model_param_dimension (param_name, python_type) VALUES %s" % insertion_string
    return insert_sql
        

def generate_model_run_db(db_path, params):
    sqliteConnection = sqlite3.connect(db_path)
    cursor = sqliteConnection.cursor()
    model_param_sql = generate_model_param_table_sql(params)
    cursor.execute(model_param_sql)
    cursor.execute(MODEL_RUN_TABLE_SQL)
    cursor.execute(PARAM_DIM_TABLE_SQL)
    param_dim_sql = generate_model_param_dim_table_sql(params)
    cursor.execute(param_dim_sql)
    sqliteConnection.commit()
    cursor.close()

ITER_PARAM_RE = re.compile(r"ITERATIVE\s+(\w+)\s+(\{.*\})")
VALID_GENERATORS = {"linspace": np.linspace,
                    "arange": np.arange,
                    "logspace": np.logspace,
                    "geomspace": np.geomspace}

def get_iterative_params(paramaters):
    fparams = flatten_dict(paramaters)#, "model_param")
    iterative_keys = []
    for key, value in fparams.items():
        if isinstance(value, str) and value.split(" ")[0].upper() == "ITERATIVE":
            iterative_keys.append(key)
    return iterative_keys

def generate_parameter_array(interative_param_value):
    match = ITER_PARAM_RE.match(interative_param_value)
    function = VALID_GENERATORS[match.group(1)]
    args = json.loads(match.group(2))
    return function(**args)

class ModelParams:
    # general idea, get an array with every parameter combination and iterate through that
    def __init__(self, parameters):
        self.parameters = parameters
        self.iterative_params = get_iterative_params(parameters)
        flat_params = flatten_dict(parameters)#, "model_param")
        parameter_arrays = [generate_parameter_array(flat_params[param]) for param in self.iterative_params]
        self.iterative_parameter_values = np.array(np.meshgrid(*parameter_arrays)).T.reshape(-1,len(parameter_arrays))
        self.current = 0

    def __iter__(self):
        return self
        
    def __next__(self):
        return self.next()
        
    def next(self):
        if self.current >= len(self.iterative_parameter_values):
            raise StopIteration
        iterative_param_values = self.iterative_parameter_values[self.current]
        params_to_return = deepcopy(self.parameters)
        for i, param in enumerate(self.iterative_params):
            param_val = iterative_param_values[i]
            working_params = params_to_return
            for key in param.split('.')[:-1]:
                working_params = working_params[key]
            working_params[param.split('.')[-1]] = param_val
        self.current += 1
        return params_to_return

def insert_model_run(cursor, params):
    flat_params = flatten_dict(params, "model_param")
    columns = str(tuple(flat_params.keys()))#.replace('[', '(').replace(']', ')')
    values = []
    for value in flat_params.values():
        if not isinstance(value, (int, float)):
            values.append(str(value))
        else:
            values.append(value)
    values = str(tuple(values))#.replace('[', '(').replace(']', ')')
    query_str = "INSERT INTO model_run_params %s VALUES %s;" % (columns, values)
    cursor.execute(query_str)

def create_model_db(db_path, param_path):
    with open(param_path, 'r') as param_f:
        params = json.load(param_f)
    model_params = ModelParams(params)
    param = model_params.next()
    generate_model_run_db(db_path, param)
    sqliteConnection = sqlite3.connect(db_path)
    cursor = sqliteConnection.cursor()
    insert_model_run(cursor, param)
    for param in model_params:
        insert_model_run(cursor, param)
    sqliteConnection.commit()
    cursor.close()


    
            
