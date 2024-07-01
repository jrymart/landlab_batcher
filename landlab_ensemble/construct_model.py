import importlib
import builtins
import json
import sqlite3
import uuid
import time

def _resolve_type(type_str):
    """
    adapted from chatgpt
    """
    if type_str == "<class 'NoneType'>":
        return type(None)
    module_class = type_str.split("'")[1].rsplit('.', 1)
    if len(module_class) == 1:
        module = builtins
        class_name = module_class[0]
    else:
        module_name, class_name = module_class
        module = importlib.import_module(module_name)
    return getattr(module, class_name)

def _ensure_type(value, ideal_type):
    if ideal_type==type(None):
        return None
    elif ideal_type in (type([]), type({})):
        return json.loads(value.replace("'", "\""))
    else:
        return ideal_type(value)

def _expand_key_into_dict(key, value, current_dict):
    split_key = key.split('.',1)
    if len(split_key)==1:
        current_dict[key] = value
        return current_dict
    else:
        try:
            next_dict = current_dict[split_key[0]]
        except KeyError:
            next_dict = {}
            current_dict[split_key[0]] = next_dict
        return _expand_key_into_dict(split_key[1], value, next_dict)
    
def expand_dict(flat_dict):
    expanded_dict = {}
    for key, value in flat_dict.items():
        _expand_key_into_dict(key, value, expanded_dict)
    return expanded_dict

def row_to_params(row, columns, types):
    row_dict = dict(zip(columns, row))
    parameter_dictionary = {k.split('.', 1)[1]: _ensure_type(v, types[k]) for k,v in row_dict.items() if k.split('.')[0]=="model_param"}
    return expand_dict(parameter_dictionary)

def get_param_types(connection):
    cursor = connection.cursor()
    param_and_type = cursor.execute("SELECT param_name, python_type FROM model_param_dimension").fetchall()
    cursor.close()
    return {k: _resolve_type(v) for k,v in param_and_type}

class ModelDispatcher:

    def __init__(self, database, model_class, out_dir="", filter=None):
        self.database = database
        self.model_class = model_class
        self.batch_id = uuid.uuid4()
        self.out_dir = out_dir
        connection = sqlite3.connect(database)
        self.parameter_types = get_param_types(connection)
        if filter:
            self.filter_statement = "%s AND model_run_id IS NULL"
        else:
            self.filter_statement = "model_run_id IS NULL "

    def dispatch_model(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        results = cursor.execute("SELECT run_param_id, * FROM model_run_params WHERE %s" % self.filter_statement).fetchone()
        run_id = results[0]
        model_parameters = results[1:]
        columns = [c[0] for c in cursor.description][1:]
        param_dict = row_to_params(model_parameters, columns, self.parameter_types)
        model_run_id = str(uuid.uuid4())
        model = self.model_class(param_dict)
        model.batch_id = self.batch_id
        model.run_id = model_run_id
        update_statement = "UPDATE model_run_params SET model_batch_id = \"%s\", model_run_id = \"%s\" WHERE run_param_id = %d" % (model.batch_id, model.run_id, run_id)
        cursor.execute(update_statement)
        start_time = time.time()
        metadata_insert_statement = "INSERT INTO model_run_metadata (model_run_id, model_batch_id, model_start_time) VALUES (\"%s\", \"%s\", %f)" % (model.run_id, model.batch_id, start_time)
        cursor.execute(metadata_insert_statement)
        connection.commit()
        cursor.close()
        model.update_until(model.run_duration, model.dt)
        end_time = time.time()
        cursor = connection.cursor()
        metadata_update_statement = "UPDATE model_run_metadata SET model_end_time = %f WHERE model_run_id = \"%s\"" %(end_time, model.run_id)
        cursor.execute(metadata_update_statement)
        connection.commit()
        output_f = "%s%s.nc" % (self.out_dir, model.run_id)
        model.grid.save(output_f)

    
