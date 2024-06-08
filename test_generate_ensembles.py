from generate_ensembles import get_iterative_params, generate_parameter_array
import json
import numpy as np
import pytest

TEST_PARAM_FILE = "model_params.json"

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
