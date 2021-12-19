"""
OData Interface

V2 implemented via https://github.com/SAP/python-pyodata
V4 implemented via https://github.com/tuomur/python-odata

TODO: v3
"""

import logging

logger = logging.getLogger(__name__)

class ODataConverter():
    def __init__(self, endpoint, dir, resource, worker, worker_id=0):
        self._endpoint = endpoint
        self._dir = dir
        self._worker = worker
        self._workerid = worker_id
        self._resource = resource
        self._profling_lines = 1000

        """ objects.csv / links.csv protocol """
        self._objects_head = {}
        self._links_head = {}

        """ see model/model.xml """
        """ 1) class """
        self._packagename = 'com.informatica.ldm.odata'
        self._model_endpoint = 'com.informatica.ldm.odata.endpoint'
        self._model_entitytype = 'com.informatica.ldm.odata.entitytype'
        self._model_entityset = 'com.informatica.ldm.odata.entityset'
        self._model_property = 'com.informatica.ldm.odata.property'

        """ 2) attribute """
        self._model_entityset_entitytype = 'com.informatica.ldm.odata.ENTITYTYPE'
        self._model_property_primarykey = 'com.infa.ldm.relational.PrimaryKeyColumn'
        self._model_property_datatype = 'com.infa.ldm.relational.Datatype'
        self._model_property_odataversion = 'com.informatica.ldm.odata.ODATAVERSION'
        self._model_property_nullable = 'com.infa.ldm.relational.Nullable'
        self._model_property_maxlength = 'com.infa.ldm.relational.DatatypeLength'
        self._model_property_fixedlength = 'com.infa.ldm.relational.DatatypeLength'
        self._model_property_precision = 'com.infa.ldm.relational.DatatypeLength'
        self._model_property_scale = 'com.infa.ldm.relational.DatatypeScale'

        """ 3) association """
        self._association_endpointentityset = 'com.informatica.ldm.odata.endpointentityset'
        self._association_entitytypeproperty = 'com.informatica.ldm.odata.entitytypeproperty'
        self._association_entitysetproperty = 'com.informatica.ldm.odata.entitysetproperty'

    def print_out_metadata_info(self):
        logger.info('... wrong calling base function ...')
    
    def convert_objects(self, force=False):
        """ create objects.csv """
        import os
        assert os.path.exists(self._dir), "given dir not found: " + str(self._dir)

        if not force:
            assert os.path.exists(self._dir + '/links.csv') == False, "links.csv exists in: " + str(self._dir)
            assert os.path.exists(self._dir + '/objects.csv') == False, "objects.csv exists in: " + str(self._dir)
        else:
            import glob
            files = glob.glob(self._dir + '/*')
            for f in files:
                os.remove(f)

        self._objects_head = {
            'class': '',
            'identity': '',
            'core.name': '',
            'core.description': '',
            self._model_entityset_entitytype: '',
            self._model_property_primarykey: '',
            self._model_property_datatype: '',
            self._model_property_odataversion: '',
            self._model_property_nullable: '',
            self._model_property_maxlength: '',
            self._model_property_fixedlength: '',
            self._model_property_precision: '',
            self._model_property_scale: ''
        }
        # do not overwrite the existing files yet
        # only do so when the conversion has succeeded.

    def convert_links(self, force=False):
        """ create links.csv """
        import os
        assert os.path.exists(self._dir), "given dir not found: " + str(self._dir)

        if not force:
            assert os.path.exists(self._dir + '/links.csv') == False, "links.csv exists in: " + str(self._dir)
            assert os.path.exists(self._dir + '/objects.csv') == False, "objects.csv exists in: " + str(self._dir)

        self._links_head = {
            'association': '',
            'fromObjectIdentity': '',
            'toObjectIdentity': ''
        }
    
    def set_profiling_lines(self, lines=100):
        self._profling_lines = lines

    def fetch_pdata(self, force=False):
        logger.info('... wrongly calling base function ...')
    
    def invoke_worker(self):
        from multiprocessing import Process
        from subprocess import Popen, PIPE
        import sys

        file = sys.argv[0]

        def exec_(i):
            p = Popen([file, '--asworker', '-f', '--asworker_id=' + str(i)])
            stdout, stderr = p.communicate()
  
        from concurrent.futures import ThreadPoolExecutor
        import time

        with ThreadPoolExecutor(max_workers=int(self._worker)) as executor:
            start = time.time()
            futures = executor.map(exec_, range(int(self._worker)))
            for future in futures:
                pass
            end = time.time()
            logger.info(f'[... PROFILING FINISHED IN {end-start} SECONDS ...]')

            import os
            import csv

            if os.path.exists(self._dir):
                with open(self._dir + f'/{self._resource}_DatasetMapping.csv', 'w') as f:
                    logger.info(f'[... CREATETING {self._dir}/{self._resource}_DatasetMapping.csv ...]')
                    writer = csv.writer(f, delimiter=',')
                    writer.writerow(['DatasetId','AssociationType','DataTypeAttribute','DatasetFilePath'])
                    f.close()
                    p = Popen([file, '--asconsumer', '-f'])
                    stdout, stderr = p.communicate()

    def profile(self, esname):
        import os
        logger.info(f'[... (WORKER {self._workerid} - PID {os.getpid()}) START PROFILING JOB: {esname}]')
    
    def create_profiling_constant_files(self, resource, force):

        import os
        import csv
        
        if os.path.exists(self._dir):
            """
            TODO: create or overwrite {Resourcename}_DatatypeMapping.csv

            Data type refer to:
            ODATA: https://www.odata.org/documentation/odata-version-2-0/overview/
            EDC: https://knowledge.informatica.com/s/article/Custom-Scanner-Profiling-in-Enterprise-Data-Catalog?language=en_US&type=external

            Since using com.infa.ldm.relational, the corresponding data type is re-sued;
            no {Resourcename}_DatatypeMapping.csv will be used.

            with open(self._dir + f'/{resource}_DatatypeMapping.csv', 'w') as f:
                print(f'[... CREATETING {self._dir}/{resource}_DatatypeMapping.csv ...]')
                writer = csv.writer(f, delimiter=',')
                writer.writerow(['CustomType','EDCType'])
                writer.writerow(['Edm.Int32','Number'])
                writer.writerow(['Edm.Boolean','Boolean'])
                writer.writerow(['Edm.Byte','Binary'])
                writer.writerow(['Edm.DateTime','DateTime'])
                writer.writerow(['Edm.Decimal','Double'])
                writer.writerow(['Edm.Double','Double'])
                writer.writerow(['Edm.Single','Double'])
                writer.writerow(['Edm.Guid','String'])
                writer.writerow(['Edm.Int16','Number'])
                writer.writerow(['Edm.Int32','Number'])
                writer.writerow(['Edm.Int64','BigInteger'])
                writer.writerow(['Edm.SByte','Binary'])
                writer.writerow(['Edm.String','String'])
                writer.writerow(['Edm.Time','DateTime'])
                writer.writerow(['Edm.DateTimeOffset','TimeStampWithTZ'])
            """

            """ create or overwrite ProfileableClassTypes.csv """
            
            with open(self._dir + '/ProfileableClassTypes.csv', 'w') as f:
                logger.info(f'[... CREATETING {self._dir}/ProfileableClassTypes.csv ...]')

                """
                TODO: seems a bug in EDC.
                If only one-liner in the file, the line is not read.
                Therefore workaround to write a dummy row in the beginning row
                """
                f.write('com.informatica.ldm.odata.AAAAA\n')
                f.write('com.informatica.ldm.odata.property')
                f.close()

        else:
            logger.info('[... NO DIR FOR PROFILING IS GIVEN...]')
    
    def prepare_dataset_mapping(self, obj):
        logger.info(obj)


class ODataConverterFactory():
    def __init__(self):
        self._converter = None
        self._root_url = None
        self._version = None
    
    def version(self):
        return self._version
    
    def prone(self, url):
        """
        ODATA Version:

        1) for v4 there is obliged declaration in <edmx:Edmx /> 4.0 or 4.
        2) for v2 and v3 there are no unified way

        """

        logger.info(f'[... {self._version} is detected ...]')

        return False
    
    def set_version(self, version):
        """
        when prone failed, manually setup the version...
        """
        if version == 'v2' or version == 'v3' or version == 'v4':
            self._version = version
        else:
            logger.info(f'[... invalid {version} given: only v2, v3 or v4 allowed ...]')
    
    def create_converter(self, endpoint, dir, resource, worker, worker_id):
        from .odata_converter_v2 import ODataConverterV2
        from .odata_converter_v4 import ODataConverterV4

        if self._version == 'v2':
            return ODataConverterV2(endpoint, dir, resource, worker, worker_id)
        elif self._version == 'v4':
            return ODataConverterV4(endpoint, dir, resource, worker, worker_id)
        else:
            logger.info('[... only v2 and v4 are supported ...]')
            return None

