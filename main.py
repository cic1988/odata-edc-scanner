#!/usr/bin/env python3

import sys

from argparse import ArgumentParser
from multiprocessing import Pool
from configparser import ConfigParser
from converter.odata_converter_v2 import ODataConverterV2

def execute(args):
    """ read initial setup from ini firstly """
    import os
    import sys

    file = sys.argv[0]
    pathname = os.path.dirname(os.path.abspath(file))

    cp = ConfigParser()
    cp.read(pathname + '/config.ini')
    root_url = cp.get('local', 'root_url')
    dir = cp.get('local', 'dir')
    worker = cp.get('local', 'worker')
    resource = cp.get('local', 'resource')

    root_url = root_url if not args.SERVICE_ROOT_URL else root_url
    dir = dir if not args.dir else dir
    worker = worker if not args.worker else worker
    resource = resource if not args.resource else resource

    """ a bit verbose """

    print('[... START SCANNING ...]')
    print(f'[... ROOT_URL: {root_url} ...]')
    print(f'[... DIR: {dir} ...]')
    print(f'[... RESORUCE NAME: {resource} ...]')
    print(f'[... NUMBER OF WORKERS: {worker} ...]')
    print(f'[... AS WORKER: {args.asworker} ...]')
    print(f'[... AS CONSUMER: {args.asconsumer} ...]')
    print(f'[... OVERWRITTEN: {args.force} ...]')
    print(f'[... VERBOSE: {args.verbose} ...]')

    converter = ODataConverterV2(root_url, dir, resource, worker, args.asworker_id)

    if args.asworker:
        print('[... WORKER START ...]')
        converter.profile()
        return
    
    if args.asconsumer:
        print('[... CONSUMER START ...]')
        converter.prepare_dataset_mapping()
        return

    if args.verbose:
        converter.print_out_metadata_info()

    converter.convert_objects(args.force)
    converter.convert_links(args.force)

    if resource:
        converter.fetch_pdata(args.force)
        converter.create_profiling_constant_files(resource, args.force)

def _parse_args(argv):
    parser = ArgumentParser()
    parser.add_argument('SERVICE_ROOT_URL', type=str, nargs='?')
    parser.add_argument('--dir', default=None, type=str, help='directory to save objects.csv and links.csv')
    parser.add_argument('--force', '-f', default=False, action='store_true', help='force overwritten the existing objects.csv and links.csv')
    parser.add_argument('--verbose', '-v', default=False, action='store_true')
    parser.add_argument('--resource', default=None, type=str, help='(profiling enabled) resource name')
    parser.add_argument('--worker', default=1, type=int, help='(profiling enabled) number of workers')
    parser.add_argument('--asworker', default=False, action='store_true', help='(profiling enabled) start process as worker')
    parser.add_argument('--asworker_id', default=1, type=int, help='(profiling enabled) start process as worker with id x')
    parser.add_argument('--asconsumer', default=False, action='store_true', help='(profiling enabled) start process as consumer')
    parser.set_defaults(func=execute)

    args = parser.parse_args(argv[1:])
    return args

def _main(argv):
    args = _parse_args(argv)
    args.func(args)

    if args.asworker:
        print(f'[... WORKER {args.asworker_id} DONE ...]')
    else:
        print(f'[... MAIN DONE ...]')

    return 0

if __name__ == '__main__':
    """ otherwise error """
    """ see: https://github.com/pyinstaller/pyinstaller/wiki/Recipe-Multiprocessing """
    """      https://stackoverflow.com/questions/46335842/python-multiprocessing-throws-error-with-argparse-and-pyinstaller"""
    import multiprocessing
    multiprocessing.freeze_support()
    sys.exit(_main(sys.argv))

