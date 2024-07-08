import importlib
import builtins
import json
import sqlite3
import uuid
import time
from dask.distributed import Client
#import multiprocessing
#from multiprocessing import set_start_method
#set_start_method("spawn")

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

class ModelSelector:

    def __init__(self, database, filter=None, limit=None):
        self.database = database
        if filter:
            self.filter_statement = "%s AND model_run_id IS NULL"
        else:
            self.filter_statement = "model_run_id IS NULL"
        if limit is not None:
            limit_statement = "LIMIT %d" % limit
        else:
            limit_statement = ""
        self.connection = sqlite3.connect(database, check_same_thread=False)
        self.parameter_types = get_param_types(self.connection)
        cursor = self.connection.cursor()
        self.select_statement = "SELECT run_param_id, * FROM model_run_params WHERE %s" % (self.filter_statement)#, limit_statement))
        cursor.execute(self.select_statement)
        self.columns = [c[0] for c in cursor.description[1:]]
        cursor.close()
        self.limit = limit
        self.current = 0

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self.limit is not None and self.current > self.limit:
            raise StopIteration
        cursor = self.connection.cursor()
        results = cursor.execute(self.select_statement).fetchone()
        cursor.close()
        if results is None:
            raise StopIteration
        run_id = results[0]
        model_parameters = results[1:]
        param_dict = row_to_params(model_parameters, self.columns, self.parameter_types)
        return run_id, param_dict

    def empty(self):
        if self.limit is not None and self.current > self.limit:
            return True
        else:
            cursor = self.connection.cursor()
            results = cursor.execute(self.select_statement).fetchone()
            cursor.close()
            if results is None:
                return True
            else:
                return False

    
def get_runner_for_pool(dispatcher):
    return lambda p: dispatcher.dispatch_model(p[0], p[1])

def make_and_run_model(model_class, batch_id, model_run_id, param_dict, out_dir):
    model = model_class(param_dict)
    model.batch_id = batch_id
    model.run_id = model_run_id
    model.update_until(model.run_duration, model.dt)
    end_time = time.time()
    output_f = "%s%s.nc" % (out_dir, model.run_id)
    model.grid.save(output_f)
    return (model.batch_id, model.run_id, end_time)

class ModelDispatcher:

    def __init__(self, database, model_class, out_dir="", filter=None, limit=None, processes=None):
        self.database = database
        self.model_class = model_class
        self.parameter_list = ModelSelector(database, filter, limit)
        self.batch_id = uuid.uuid4()
        self.out_dir = out_dir
        connection = sqlite3.connect(database, check_same_thread=False)
        self.parameter_types = get_param_types(connection)
        self.filter = filter
        if filter:
            self.filter_statement = "%s AND model_run_id IS NULL"
        else:
            self.filter_statement = "model_run_id IS NULL "
        if processes is not None:
            self.client = Client(threads_per_worker=1, n_workers=processes)
            #multiprocessing.set_start_method('spawn')
            #self.pool = multiprocessing.Pool(processes)
            #self.pool_runner = get_runner_for_pool(self)
        self.processes = processes

    
    def get_unran_parameters(self):
        connection = sqlite3.connect(self.database, check_same_thread=False)
        cursor = connection.cursor()
        results = cursor.execute("SELECT run_param_id, * FROM model_run_params WHERE %s" % self.filter_statement).fetchone()
        run_id = results[0]
        model_parameters = results[1:]
        columns = [c[0] for c in cursor.description][1:]
        param_dict = row_to_params(model_parameters, columns, self.parameter_types)
        return run_id, param_dict

    def run_a_model(self):
        try:
            run_id, param_dict = self.parameter_list.next()
            self.dispatch_model(run_id, param_dict)
        except StopIteration:
            self.end_batch()

    def end_batch(self):
        print("no more to run")

    def run_all(self):
        if self.processes is not None:
            #print(__name__)
            #self.pool.map(self.pool_runner, self.parameter_list)
            self.run_models_on_dask()
        else:
            for run_id, param_dict in self.parameter_list:
                #run_id = row[0]
                #param_dict = row[1:]
                self.dispatch_model(run_id, param_dict)
        self.end_batch()

    def get_unfinished_runs(self):
        connection = sqlite3.connect(self.database, check_same_thread=False)
        cursor = connection.cursor()
        selection_statement = "SELECT model_run_id FROM model_run_metadata WHERE model_start_time IS NOT NULL AND model_end_time IS NULL"
        if self.filter:
            selection_statement = "%s AND %s" % (selection_statement, self.filter)
        unfinished = [r[0] for r in cursor.execute(selection_statement).fetchall()]
        cursor.close()
        if len(unfinished)==0:
            return None
        else:
            return unfinished

    def reset_model(self, model_run_id, clear_metadata=True):
        connection = sqlite3.connect(self.database, check_same_thread=False)
        cursor = connection.cursor()
        update_statement = "UPDATE model_run_params SET model_run_id = NULL, model_batch_id = NULL WHERE model_run_id = \"%s\"" % model_run_id
        cursor.execute(update_statement)
        if clear_metadata:
            delete_statement = "DELETE FROM model_run_metadata WHERE model_run_id = \"%s\"" % model_run_id
            cursor.execute(delete_statement)
        connection.commit()
        cursor.close()

    def clean_unfinished_runs(self, clear_metadata=True):
        unfinished_runs = self.get_unfinished_runs()
        if unfinished_runs:
            for model_run in unfinished_runs:
                self.reset_model(model_run, clear_metadata)

    def run_models_on_dask(self):
        model_runs = []
        parameter_list_empty = False
        for _ in range(2*self.processes):
            try:
                run_id, param_dict = self.parameter_list.next()
                model_run = self.dispatch_model_to_dask(run_id, param_dict)
                model_runs.append(model_run)
            except StopIteration:
                break
        while True:
            try:
                index = [model.status for model in model_runs].index('finished')
                finished_run = model_runs.pop(index)
                self.record_finished_run(*finished_run.result())
                if parameter_list_empty and len(model_runs)==0:
                    break
                try:
                    run_id, param_dict = self.parameter_list.next()
                    model_run = self.dispatch_model_to_dask(run_id, param_dict)
                    model_runs.append(model_run)
                except StopIteration:
                        parameter_list_empty = True
            except ValueError:
                pass
                
                
    def record_finished_run(self, batch_id, run_id, end_time):
        connection = sqlite3.connect(self.database, check_same_thread=False)
        cursor = connection.cursor()
        metadata_update_statement = "UPDATE model_run_metadata SET model_end_time = %f WHERE model_run_id = \"%s\"" %(end_time, run_id)
        cursor.execute(metadata_update_statement)
        connection.commit()
        cursor.close()

    def runner(self, run_id, params, model_run_id):
        return self.build_and_run_model(run_id, params, model_run_id)
                
    def dispatch_model_to_dask(self, run_id, param_dict):
        #runner = lambda run_id, params, model_run_id: self.build_and_run_model(run_id, params, model_run_id)
        model_run_id = str(uuid.uuid4())
        start_time = time.time()
        self.set_model_as_in_progress(self.batch_id, model_run_id, run_id, start_time)
        model_run = self.client.submit(make_and_run_model, self.model_class, self.batch_id, model_run_id, param_dict, self.out_dir)
        return model_run

    def set_model_as_in_progress(self, model_batch_id, model_run_id, param_run_id, start_time):
        connection = sqlite3.connect(self.database, check_same_thread=False)
        cursor = connection.cursor()
        update_statement = "UPDATE model_run_params SET model_batch_id = \"%s\", model_run_id = \"%s\" WHERE run_param_id = %d" % (model_batch_id, model_run_id, param_run_id)
        cursor.execute(update_statement)
        start_time = time.time()
        metadata_insert_statement = "INSERT INTO model_run_metadata (model_run_id, model_batch_id, model_start_time) VALUES (\"%s\", \"%s\", %f)" % (model_run_id, model_batch_id, start_time)
        cursor.execute(metadata_insert_statement)
        connection.commit()
        cursor.close()

    def build_and_run_model(self, run_id, param_dict, model_run_id):
        model = self.model_class(param_dict)
        model.batch_id = self.batch_id
        model.run_id = model_run_id
        model.update_until(model.run_duration, model.dt)
        end_time = time.time()
        output_f = "%s%s.nc" % (self.out_dir, model.run_id)
        model.grid.save(output_f)
        return (model.batch_id, model.run_id, end_time)
    
    def dispatch_model(self, run_id, param_dict):
        print("dispatching model %d" % run_id)
        connection = sqlite3.connect(self.database, check_same_thread=False)
        cursor = connection.cursor()
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

    
