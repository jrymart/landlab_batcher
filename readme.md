# Landlab Ensembles
This is python code to help manage large numbers of [Landlab](https://github.com/landlab/landlab) model runs over a parameter space.  It extends the landlab model class idea introduced [here](https://github.com/gregtucker/bigantr_model/blob/main/model_base/model_base.py), particularly allowing for a json configuration file where certain paremeters are numpy array creation routines.  This code contains two components:
1. `generate_ensembles`- to generate a sqlite database containing information about all of the possible model runs.
2. `construct_mmodel` - to generate and run models based on entries in the database.

## Model Database Generation
The model database is generated from a json file like so:
```
{
    "grid": {
	"source": "create",
	"create_grid": {
	   "RasterModelGrid": [
		[41, 5],
		{"xy_spacing": 5}
	   ]}},
    "clock": {"start": 0.0, "stop": 100000, "step": 1250},
    "output": {"plot_times": [10000001], "save_times": [10000001], "report_times": [10000001],
	      "save_path": "model_run", "fields": null, "plot_to_file": true},
    "baselevel": {"uplift_rate": "ITERATIVE linspace {\"start\": 0.01, \"stop\": 0.1, \"num\": 3}"},
    "diffuser": {"D": "ITERATIVE linspace {\"start\": 0.01, \"stop\": 0.1, \"num\": 10}"},
    "streampower": {"k": "ITERATIVE linspace {\"start\": 0.001, \"stop\": 0.01, \"num\": 10}",
		   "m": 0, "n": 2, "threshold": 2}
}
```
This should contain all of the necessary details to define your landlab model, in the style of Greg Tucker's landlab model base class.  However, parameters can be given values that start with the keyword `ITERATIVE` to define a parameter to iterate over.  The syntax for iterative paremters is a string of the following form: "ITERATIVE <numpy array creation function name> <parameter dictionary>".  Two caveates:
1. Currently the parameter dictionary needs to be part of the string, and keys need to be escaped double quotes.  This is to simplify some of the json decoding.
2. Only the following numpy creation routines are supported:
   - arange
   - linspace
   - logspace
   - geomspace
While I'd like for users to be able to pass arbitrary code to generate dynamic parameters, this would mean the program could execute arbitrary code, which would be a significiant security risk.

This creates a sqlite database with the following three tables: `model_run_params`, `model_run_metadata`, and `model_param_dimension`.

### `model_run_params`
This conrains a row for every possible parameter combination with the following columns:
| column name | description |
| ----------- | ----------- |
| `run_param_id`      | primary key for this table |
| `model_run id` | uuid describing this specific model run (empty until run is executed) |
| `model_batch_id` | uuid describing the batch of models that this specific model run occured under (empty until run is executed) |

It also contains a column for every parameter defined in the json file used in creation.  Each parameter name is prefixed with `model_param` and nested dictionarys would be flattened  Using the above example, it would contain the following fields (and more):
| example column | example value |
| -------------- | ------------- |
| `model_param.grid.source` | `"create"` |
| `model_param.grid.create_grid.RasterModelGrid` | `"[41, 5], {\"xy_spacing\": 5}"` |
| `model_param.clock.start` | `0.0` |
| `model_param.clock.stop` | `100000`|
| `model_param.baselevel.uplift_rate` | `0.01` |

The above json would create 300 rows in the paramter table as there are 300 possible paramter combinations

### `model_run_metadata`
This table contains information about model runs that are created based on the `model_run_params` table.  Currently it contains the following columns:
| column name | description |
| ----------- | ----------- |
| `run_id` | primary key for this table |
| `model_run_id` | uuid describing this specific model run |
| `model_batch_id` | uuid describing the batch od models that this run occured under |
| `model_start_time` | unix time stamp of when the model run was started |
| `model_end_time` | unix time stamp describing when the model run finished |

This table will hopefully expand as we discover other metadata that is useful to track (resources expended, etc).

### `model_param_dimension`
This table contains information about the model parameters.  Currently it just contains what the python type of each parameter is to aid in reconstruction of the parameters from the `model_run_params`table for model creation.  Currently it contains the following columns:
| column name | description |
| `param_name` | the flattened name of the parameter |
| `python_type` | the type of the value of the paramter (i.e. `"<class int">) |

This table will hopefully expand as we discover other aspects of paramters that are useful to track.

## Model Creation from the database:
This is where `construct_model` comes in.  It defines a class `ModelDispatcher` which takes in a sqlite database and a corresponding model class that extends Greg Tucker's landlab BaseModel.  The `ModelDispatcher` class has the `dispatch_model` function which selects an unrun parameter combination, creates the model, and runs it.  It then saves the output landlab grid as a netcdf with the model run id as the filename.  This part is under the most active development to make it more feature rich.

## To Do
- Better tests (currently all tests exist in `test_generate_ensembles`) and especially tests for the `construct_model component.
- Inline documentation
- multiprocessing of models
- single netcdf outout for batches
- commandline utility for table generation and model dispatch
- calculted scientific outputs to be stored in a table
