from landlab_ensemble.generate_ensembles import get_dynamic_params, generate_iterative_parameter_array, generate_random_parameter_array, create_model_db, flatten_dict
from landlab_ensemble.construct_model import _resolve_type
import landlab_ensemble.generate_ensembles as ge
import json
import numpy as np
import pytest
import sqlite3
import pathlib
import re

TEST_PARAM_FILE = "model_params_for_pytest.json"
TEST_DB_FILE = "test.db"
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


class TestAuxilarlyFunctions:
    def
    
    def test_resolve_type():
        assert type(1) == _resolve_type(str(type(1)))

    def test_find_iterative_params():
        with open(TEST_PARAM_FILE, 'r') as param_f:
            params = json.load(param_f)
        iterative_keys = [p[0] for p in get_dynamic_params(params)]
        assert "baselevel.uplift_rate" in iterative_keys
        assert "diffuser.D" in iterative_keys
        assert "streampower.k" in iterative_keys
        assert "seed" in iterative_keys

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

# TODO break into multiple tests
def make_test_db():
    
    create_model_db(TEST_DB_FILE, TEST_PARAM_FILE)
    


def test_db_creation():
    create_model_db(TEST_DB_FILE, TEST_PARAM_FILE)
    sqliteConnection = sqlite3.connect(TEST_DB_FILE)
    cursor = sqliteConnection.cursor()
    run_param_query = cursor.execute("select * from model_run_params")
    actual_columns = [d[0] for d in run_param_query.description]
    for col in PARAM_COLUMNS:
        assert col in actual_columns
    metadata_query = cursor.execute("select * from model_run_metadata")
    actual_columns = [d[0] for d in metadata_query.description]
    for col in METADATA_COLUMNS:
        assert col in actual_columns
    #pathlib.Path(TEST_DB_FILE).unlink()
    with open(TEST_PARAM_FILE, 'r') as param_file:
        param_str = param_file.read()
    number_extractor = re.compile(r"\\\"(size|num)\\\":\s+\[?(\d+)")
    total_runs = np.prod([int(match[1]) for match in number_extractor.findall(param_str)])
    assert cursor.execute("SELECT COUNT(run_param_id) from model_run_params").fetchone()[0] == total_runs
    col_str = str(PARAM_COLUMNS).replace("'", "\"")[1:-1]
    for u in np.linspace(0.01, 0.1,3):
        for D in np.linspace(0.01, 0.1, 10):
            for k in np.linspace(0.001, 0.01, 10):
                # TODO fix/or otherwise adjust rounding
                query = "SELECT %s FROM model_run_params WHERE ROUND(\"model_param.baselevel.uplift_rate\",4) = %.4f AND ROUND(\"model_param.diffuser.D\", 4) = %.4f AND ROUND(\"model_param.streampower.k\", 4) = %.4f" % (col_str, u, D, k)
                result = cursor.execute(query).fetchone()
                ideal_result = ["create", "[[41, 5], {'xy_spacing': 5}]", 0, 100000, 1250, "[10000001]",
                                "[10000001]", "[10000001]", "model_run", "None", 1, u, D, k]
                for i, v, in enumerate(ideal_result):
                    assert v == result[i]

def test_multi_test():
    sub_test1()
    sub_test2()
    
def sub_test1():
    assert True

def sub_test2():
    assert True
