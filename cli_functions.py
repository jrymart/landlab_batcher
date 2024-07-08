from landlab_ensemble import generate_ensembles as ge
from landlab_ensemble import construct_model as cm
import argparse
import os
import sys
import importlib
from multiprocessing import set_start_method
#set_start_method("spawn")
import multiprocessing
from dask.distributed import Client, progress


def create(args):
    input_template = args.template
    output_db = args.output
    if not os.path.exists(input_template):
        raise argparse.ArgumentTypeError(f"The provided template file, '{input_template}' could not be found.")
    if os.path.exists(output_db):
        raise argparse.ArgumentTypeError(f"The provided output database file, '{output_db}' already exists.")
    ge.create_model_db(output_db, input_template)

def dispatch(args):
    if not os.path.exists(args.database):
        raise argparse.ArgumentTypeError(f"The provided database file, `{args.database}` could not be found.")
    module, model = args.model.rsplit('.',1)
    model = getattr(importlib.import_module(module), model)
    dispatcher = cm.ModelDispatcher(args.database, model, args.od, args.filter, args.n, args.processes)
    if args.clean:
        dispatcher.clean_unfinished_runs()
    model_runner = lambda id, params: dispatcher.dispatch_model(id, params)
    if args.processes:
        models = []
        client = Client(threads_per_worker=1, n_workers = args.processes)
        print("created DASK client: %s" % client)
        for _ in range(args.processes):
            run_id, param_dict = dispatcher.parameter_list.next()
            f = client.submit(model_runner, run_id, param_dict) # submitted initial batch
            models.append(f)
        while not dispatcher.parameter_list.empty():
            try:
                index = [model.status for model in models].index('finished')
                models.pop(index)
                next_model_id, next_model_params  = dispatcher.parameter_list.next()
                f = client.submit(model_runner, next_model_id, next_model_params)
                print("queued model %d" % next_model_id)
            except ValueError:
                pass
        
    else:
        dispatcher.run_all()
#    if __name__ == '__main__':
#        dispatcher.run_all()
