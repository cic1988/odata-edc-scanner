#!/usr/bin/env python3

import sys
import logging

from argparse import ArgumentParser
from multiprocessing import Pool
from configparser import ConfigParser
from converter.odata_converter import ODataConverter, ODataConverterFactory
from converter.cdgc_adaptor import CDGCAdaptor

logger = logging.getLogger(__name__)

class ODataConverterParameter():
    def __init__(self, odata_version, dir, worker, resource):
        self.local = {
            'odata_version': odata_version,
            'dir': dir,
            'worker': worker,
            'resource': resource
        }

        self.endpoints = {}
    
    def load_endpoints(self, config):
    
        for endpoint in config.sections():
            endpoint_name = ''

            if endpoint == 'local':
                if len(config.sections()) == 1:
                    endpoint_name = 'Endpoint'
                else:
                    continue
            elif endpoint != 'local':
                endpoint_name = endpoint

            self.endpoints[endpoint_name] = {
                'root_url': config.get(endpoint, 'root_url'),
                'username': config.get(endpoint, 'username'),
                'password': config.get(endpoint, 'password'),
                'profiling_lines': config.get(endpoint, 'profiling_lines', fallback=1000),
                'profiling_filter': config.get(endpoint, 'profiling_filter')
            }

def execute(args):
    """ read initial setup from ini firstly """
    import os
    import sys

    file = sys.argv[0]
    pathname = os.path.dirname(os.path.abspath(file))

    cp = ConfigParser()
    cp.read(pathname + '/config.ini')

    odata_version = cp.get('local', 'odata_version')
    dir = cp.get('local', 'dir')
    worker = cp.get('local', 'worker', fallback=1)
    resource = cp.get('local', 'resource', fallback=None)

    parameter = ODataConverterParameter(
        odata_version,
        dir,
        worker,
        resource
    )
    parameter.load_endpoints(cp)

    """
    TODO: fix the commandline parser

    root_url = root_url if not args.SERVICE_ROOT_URL else root_url
    dir = dir if not args.dir else dir
    worker = worker if not args.worker else worker
    resource = resource if not args.resource else resource
    """

    """ a bit verbose """
    logger.info('[... START SCANNING ...]')
    logger.info(f'[... ODATA_VERSION: {odata_version} ...]')
    logger.info(f'[... DIR: {dir} ...]')
    logger.info(f'[... RESORUCE NAME: {resource} ...]')
    logger.info(f'[... NUMBER OF WORKERS: {worker} ...]')
    logger.info(f'[... AS WORKER: {args.asworker} ...]')
    logger.info(f'[... AS CONSUMER: {args.asconsumer} ...]')
    logger.info(f'[... OVERWRITTEN: {args.force} ...]')
    logger.info(f'[... VERBOSE: {args.verbose} ...]')

    object_files = []
    link_files = []

    for key, endpoint in parameter.endpoints.items():

        factory = ODataConverterFactory()
        factory.set_version(odata_version)
        converter = factory.create_converter({
            'endpoint': key,
            'root_url': endpoint['root_url'],
            'username': endpoint['username'],
            'password': endpoint['password'],
            'dir': dir,
            'resource': resource,
            'worker': worker,
            'worker_id': args.asworker_id
        })
        converter.set_profiling_lines(endpoint['profiling_lines'])
        converter.set_profiling_filter(endpoint['profiling_filter'])

        if args.asworker:
            logger.info('[... WORKER START ...]')
            converter.profile()
            return
    
        if args.asconsumer:
            logger.info('[... CONSUMER START ...]')
            converter.prepare_dataset_mapping()
            return

        if args.verbose:
            logging.basicConfig(level=logging.DEBUG)
            converter.print_out_metadata_info()

        logger.info(f'[... ENDPOINT: {key} ...]')
        logger.info(f'[... ROOT_URL: {endpoint["root_url"]} ...]')
        logger.info(f'[... PROFILING LINES: {endpoint["profiling_lines"]} ...]')
        logger.info(f'[... PROFILING FILTER: {endpoint["profiling_filter"]} ...]')

        """
        initial check only
        """
        if list(parameter.endpoints.keys()).index(key) == 0:
            converter.convert_objects(args.force)
            converter.convert_links(args.force)
        else:
            converter.convert_objects(False)
            converter.convert_links(False)
        
        """
        rename files
        """
        import os
        os.rename(dir + '/objects.csv', dir + f'/objects-{key}.csv')
        os.rename(dir + '/links.csv', dir + f'/links-{key}.csv')
        object_files.append(dir + f'/objects-{key}.csv')
        link_files.append(dir + f'/links-{key}.csv')

        if resource:
            converter.fetch_pdata(args.force)

    '''
    merge object files
    TODO: robustness 1) header check 2) column matching 3) consider to use pandas
    '''
    object_files_merged = open(dir + '/objects.csv', 'a')
    header = ''
    header_set = False

    for object_file in object_files:
        csv_in = open(object_file)
        for line in csv_in:

            if not header_set:
                header = line
                header_set = True
            else:
                if line.startswith(header):
                    continue
            object_files_merged.write(line)
    
        csv_in.close()
        logger.info(f'[... MERGED {object_file} ...]')

    object_files_merged.close()

    '''
    merge link files
    TODO: robustness 1) header check 2) column matching 3) consider to use pandas
    '''
    object_links_merged = open(dir + '/links.csv', 'a')
    header = ''
    header_set = False

    for link_file in link_files:
        csv_in = open(link_file)
        for line in csv_in:

            if not header_set:
                header = line
                header_set = True
            else:
                if line.startswith(header):
                    continue
            object_links_merged.write(line)
    
        csv_in.close()
        logger.info(f'[... MERGED {link_file} ...]')

    object_links_merged.close()

    converter.zip_metadata()

    adaptor = CDGCAdaptor(converter)
    adaptor.convert_csv()

    if resource:
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
        logger.info(f'[... WORKER {args.asworker_id} DONE ...]')
    else:
        logger.info(f'[... MAIN DONE ...]')

    return 0

if __name__ == '__main__':
    """
    multiprocessing.freeze_support() otherwise in error pyinstaller, see:
    - https://github.com/pyinstaller/pyinstaller/wiki/Recipe-Multiprocessing
    - https://stackoverflow.com/questions/46335842/python-multiprocessing-throws-error-with-argparse-and-pyinstaller
    """
    import multiprocessing
    multiprocessing.freeze_support()

    """
    for release build
    """
    import sys, os
    os.environ["PATH"] += os.pathsep + os.path.join(os.getcwd())

    """
    logging
    """
    logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    sys.exit(_main(sys.argv))

