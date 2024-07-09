from landlab_ensemble import generate_ensembles as ge
from landlab_ensemble import construct_model as cm
import argparse
import os
import importlib

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
    dispatcher.run_all()
