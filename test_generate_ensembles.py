from generate_ensembles.generate_ensembles import get_iterative_params, generate_parameter_array, create_model_db
import json
import numpy as np
import pytest
import sqlite3
import pathlib

TEST_PARAM_FILE = "model_params.json"
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


def test_find_iterative_params():
    with open(TEST_PARAM_FILE, 'r') as param_f:
        params = json.load(param_f)
    iterative_keys = get_iterative_params(params)
    assert "baselevel.uplift_rate" in iterative_keys
    assert "diffuser.D" in iterative_keys
    assert "streampower.k" in iterative_keys

def test_generate_parameter_array():
    assert np.array_equal(generate_parameter_array("ITERATIVE linspace {\"start\": 0, \"stop\": 10, \"num\": 5}"),
                          np.linspace(start=0, stop=10, num=5))
    assert np.array_equal(generate_parameter_array("ITERATIVE arange {\"start\": 0, \"stop\": 10, \"step\": 3}"),
                          np.arange(start=0, stop=10, step=3))
    assert np.array_equal(generate_parameter_array("ITERATIVE logspace {\"start\": 1, \"stop\": 2, \"num\": 100}"),
                          np.logspace(start=1, stop=2, num=100))
    assert np.array_equal(generate_parameter_array("ITERATIVE geomspace {\"start\": 1, \"stop\": 3, \"num\": 75}"),
                          np.geomspace(start=1, stop=3, num=75))
    with pytest.raises(KeyError):
        generate_parameter_array("ITERATIVE evil_function {\"bad_param\": 1, \"good_param\": 0}")

# TODO break into multiple tests
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
    assert cursor.execute("SELECT COUNT(run_param_id) from model_run_params").fetchone()[0] == 1000
    col_str = str(PARAM_COLUMNS).replace("'", "\"")[1:-1]
    for u in np.linspace(0, 0.1, 10):
        for D in np.linspace(0, 0.1, 10):
            for k in np.linspace(0, 0.01, 10):
                # TODO fix/or otherwise adjust rounding
                query = "SELECT %s FROM model_run_params WHERE ROUND(\"model_param.baselevel.uplift_rate\",4) = %.4f AND ROUND(\"model_param.diffuser.D\", 4) = %.4f AND ROUND(\"model_param.streampower.k\", 4) = %.4f" % (col_str, u, D, k)
                result = cursor.execute(query).fetchone()
                ideal_result = ["create", "[[41, 5], {'xy_spacing': 5}]", 0, 1000000, 1250, "[10000001]",
                                "[10000001]", "[10000001]", "model_run", "None", 1, u, D, k]
                for i, v, in enumerate(ideal_result):
                    assert v == result[i]
                
