
import json
import numpy as np
import pytest
import sqlite3
import pathlib
import re
import importlib
import netCDF4
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from landlab_ensemble.generate_ensembles import get_dynamic_params, generate_iterative_parameter_array, generate_random_parameter_array, create_model_db, flatten_dict
from landlab_ensemble.construct_model import _resolve_type
import landlab_ensemble.construct_model as cm
import landlab_ensemble.generate_ensembles as ge

TEST_MODEL_MODULE = "diffusion_streampower_lem"
TEST_MODEL = "SimpleLem"
TRUE_TEST_OUTPUT_DIR = "tests/true_test_output"
TEST_OUTPUT_DIR = "tests/test_output"
TEST_PARAM_FILE = os.path.abspath("tests/model_params_for_pytest.json")
TEST_DB_FILE = os.path.abspath("tests/test.db")
TRUE_DB_FILE = "tests/true_test.db"
PARAM_COLUMNS = ["model_param.grid.source",
                 "model_param.grid.create_grid.RasterModelGrid",
                 #"model_param.grid.create_grid.xy_spacing",
                 "model_param.clock.start",
                 "model_param.clock.stop",
                 "model_param.clock.step",
                 "model_param.output.plot_times",
                 "model_param.output.save_times",
                 "model_param.output.report_times",
                 "model_param.output.save_path",
                 "model_param.output.fields",
                 "model_param.output.plot_to_file",
                 "model_param.baselevel.uplift_rate",
                 "model_param.diffuser.D",
                 "model_param.streampower.k",
                 "run_param_id",
                 "model_run_id",
                 "model_batch_id"]

METADATA_COLUMNS = ["run_id",
                    "model_run_id",
                    "model_batch_id",
                    "model_start_time",
                    "model_end_time"]


def test_resolve_type():
    assert type(1) == _resolve_type(str(type(1)))

def test_find_iterative_params():
    with open(TEST_PARAM_FILE, 'r') as param_f:
        params = json.load(param_f)
    iterative_keys = [p[0] for p in get_dynamic_params(params)]
    assert "baselevel.uplift_rate" in iterative_keys
    assert "diffuser.D" in iterative_keys
    assert "streampower.k" in iterative_keys
    #assert "seed" in iterative_keys

def test_generate_iterative_parameter_array():
    assert np.array_equal(generate_iterative_parameter_array("ITERATIVE linspace {\"start\": 0, \"stop\": 10, \"num\": 5}"),
                            np.linspace(start=0, stop=10, num=5))
    assert np.array_equal(generate_iterative_parameter_array("ITERATIVE arange {\"start\": 0, \"stop\": 10, \"step\": 3}"),
                            np.arange(start=0, stop=10, step=3))
    assert np.array_equal(generate_iterative_parameter_array("ITERATIVE logspace {\"start\": 1, \"stop\": 2, \"num\": 100}"),
                            np.logspace(start=1, stop=2, num=100))
    assert np.array_equal(generate_iterative_parameter_array("ITERATIVE geomspace {\"start\": 1, \"stop\": 3, \"num\": 75}"),
                            np.geomspace(start=1, stop=3, num=75))
    with pytest.raises(KeyError):
        generate_iterative_parameter_array("ITERATIVE evil_function {\"bad_param\": 1, \"good_param\": 0}")

def test_generate_random_parameter_array():
    rng1 = np.random.default_rng(1)
    rng2 = np.random.default_rng(1)
    assert np.array_equal(generate_random_parameter_array("RANDOM integers {\"low\": 7, \"high\": 12345, \"size\": [10]}", rng1),
                            rng2.integers(low=7, high=12345, size=(10)))
    assert np.array_equal(generate_random_parameter_array("RANDOM random {\"shifter\": 5, \"scaler\": 10, \"size\": [10]}", rng1),
                            rng2.random(size=(10))*10+5)
    assert np.array_equal(generate_random_parameter_array("RANDOM poisson {\"size\": [10]}", rng1),
                            rng2.poisson(size=(10)))

def get_tables(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [table[0] for table in cursor.fetchall()]

def test_db_creation():
    create_model_db(TEST_DB_FILE, TEST_PARAM_FILE)
    test_connection = sqlite3.connect(TEST_DB_FILE)
    true_connection = sqlite3.connect(TRUE_DB_FILE)
    test_cursor = test_connection.cursor()
    true_cursor = true_connection.cursor()
    true_tables = get_tables(true_cursor)
    for table in true_tables:
        true_cursor.execute(f"SELECT * FROM {table}")
        true_data = true_cursor.fetchall()
        test_cursor.execute(f"SELECT * FROM {table}")
        test_data = test_cursor.fetchall()
        assert true_data == test_data
    test_connection.close()
    true_connection.close()
    # delete TEST_DB_FILE
    os.remove(TEST_DB_FILE)

def test_model_running():
    model = getattr(importlib.import_module(TEST_MODEL_MODULE), 
                    TEST_MODEL)
    dispatcher = cm.ModelDispatcher(TEST_DB_FILE, model, TEST_OUTPUT_DIR)
    dispatcher.parameter_list.filter = ""
    dispatcher.run_all()
    for model_run in os.listdir(TRUE_TEST_OUTPUT_DIR):
        true_model = os.path.join(TRUE_TEST_OUTPUT_DIR, model_run)
        test_model = os.path.join(TEST_OUTPUT_DIR, model_run)
        assert compare_netcdf_files(true_model, test_model), "Model does not match true."
    

def compare_netcdf_files(file1, file2):
    with netCDF4.Dataset(file1, 'r') as nc1, netCDF4.Dataset(file2, 'r') as nc2:
        # Compare dimensions
        if nc1.dimensions.keys() != nc2.dimensions.keys():
            return False
        for dim in nc1.dimensions:
            if len(nc1.dimensions[dim]) != len(nc2.dimensions[dim]):
                return False

        # Compare variables
        if nc1.variables.keys() != nc2.variables.keys():
            return False
        for var in nc1.variables:
            if not np.array_equal(nc1.variables[var][:], nc2.variables[var][:]):
                return False

        # Compare global attributes
        if nc1.ncattrs() != nc2.ncattrs():
            return False
        for attr in nc1.ncattrs():
            if getattr(nc1, attr) != getattr(nc2, attr):
                return False

    return True