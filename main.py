#!/usr/bin/env python3

import sys

from argparse import ArgumentParser
from getpass import getpass
from converter.odata_converter_v2 import ODataConverterV2

def execute(args):
    converter = ODataConverterV2(args.SERVICE_ROOT_URL)

    if args.verbose:
        converter.print_out_metadata_info()
    converter.convert_objects(args.dir, args.force)
    converter.convert_links(args.dir, args.force)

    if args.resource:
        converter.fetch_pdata(args.dir, args.force)

def _parse_args(argv):
    parser = ArgumentParser()
    parser.add_argument('SERVICE_ROOT_URL', type=str)
    parser.add_argument('--dir', default=None, type=str, help='directory to save objects.csv and links.csv')
    parser.add_argument('--force', '-f', default=False, action='store_true', help='force overwritten the existing objects.csv and links.csv')
    parser.add_argument('--verbose', '-v', default=False, action='store_true')
    parser.add_argument('--resource', default=None, type=str, help='(profiling enabled) resource name')
    parser.set_defaults(func=execute)

    args = parser.parse_args(argv[1:])
    return args


def _main(argv):
    args = _parse_args(argv)
    args.func(args)
    print('[... done ...!]')
    return 0


if __name__ == '__main__':
    sys.exit(_main(sys.argv))

