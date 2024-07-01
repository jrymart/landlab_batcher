from landlab_ensemble import generate_ensembles as ge
from landlab_ensemble import construct_model as cm
import argparse
import os
import sys

def create(args):
    input_template = args.template
    output_db = args.output
    if not os.path.exists(input_template):
        raise argparse.ArgumentTypeError(f"The provided template file, '{input_template}' could not be found.")
    if os.path.exists(output_db):
        raise argparse.ArgumentTypeError(f"The provided output database file, '{output_db}' already exists.")
    ge.create_model_db(output_db, input_template)

def dispatch(args):
    database = args.database
    if not os.path.exists(database):
        raise argparse.ArgumentTypeError(f"The provided database file, `{database}` could not be found.")
    print('This feature is currently unimplented')

def main():
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
    parse_dispatch.add_argument('-f', '--filter')
    parse_dispatch.add_argument('-n')
    parse_dispatch.set_defaults(func=dispatch)

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    sys.exit(main())
