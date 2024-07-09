from cli_functions import create, dispatch
import argparse

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
    parse_dispatch.add_argument('-m', '--model')
    parse_dispatch.add_argument('-f', '--filter')
    parse_dispatch.add_argument('-n', type=int)
    parse_dispatch.add_argument('-p', '--processes', type=int)
    parse_dispatch.add_argument('-od')
    parse_dispatch.add_argument('-c', '--clean', action='store_true')
    parse_dispatch.set_defaults(func=dispatch)

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
