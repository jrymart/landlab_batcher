from cli_functions import create, dispatch
import argparse
from multiprocessing import set_start_method
import importlib
import multiprocessing
import os
from landlab_ensemble import construct_model as cm
import sys

if __name__ == '__main__':
    set_start_method("spawn")
    parser = argparse.ArgumentParser(
        description="a CLI for generate model parameter databases and running landlab models based on them",
        usage=""" model_control <command> [<args>]

              The possible commands are:
              createdb    Create a sqlite database based on a model configuration file
              dispatch    Create and run landlab models based on a parameter database
              """)
    subparsers = parser.add_subparsers()
    parse_create = subparsers.add_parser("createdb")
    parse_dispatch = subparsers.add_parser("dispatch")
    parse_create.add_argument('-t', '--template')
    parse_create.add_argument('-o', '--output')
    parse_create.set_defaults(func=create)

    parse_dispatch.add_argument('-d', '--database')
    parse_dispatch.add_argument('-m', '--model')
    parse_dispatch.add_argument('-f', '--filter')
    parse_dispatch.add_argument('-n', type=int)
    parse_dispatch.add_argument('-p', '--processes', type=int)
    parse_dispatch.add_argument('-od')
    parse_dispatch.add_argument('-c', '--clean', action='store_true')
    parse_dispatch.add_argument('-u', '--unlock', action='store_true')
    parse_dispatch.set_defaults(func=dispatch)

    args = parser.parse_args()
    if args.func == dispatch and args.processes:
        if not os.path.exists(args.database):
            raise argparse.ArgumentTypeError(f"The provided database file, `{args.database}` could not be found.")
        module, model = args.model.rsplit('.',1)
        model = getattr(importlib.import_module(module), model)
        dispatcher = cm.ModelDispatcher(args.database, model, args.od, args.filter, args.n, args.processes)
        with multiprocessing.Pool(args.processes) as pool:
            print("processign with pool: %s" % pool)
            while not dispatcher.parameter_list.is_empty():
                model_batch = [(dispatcher.model_class, dispatcher.batch_id, r[0], r[1]) for r in dispatcher.parameter_list.get_more_chunks()]
                #breakpoint()
                finished_runs = []
                finished_runs = [(pool.starmap(cm.make_and_run_model, model_batch, chunksize=1))]
                finished_runs = finished_runs[0]
                print("writing %d runs" % len(finished_runs))
                #breakpoint()
                dispatcher.write_finished_runs(finished_runs)
    args.func(args)


#if __name__ == '__main__':
#    set_start_method("spawn")
#    sys.exit(main())
