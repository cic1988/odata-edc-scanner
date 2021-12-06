""" OData Interface """
""" V2 implemented via https://github.com/SAP/python-pyodata """
""" TODO: V4 open """

class ODataConverter():
    def __init__(self, endpoint, dir, resource, worker, worker_id=0):
        self._endpoint = endpoint
        self._dir = dir
        self._worker = worker
        self._workerid = worker_id
        self._resource = resource

        """ objects.csv / links.csv protocol """
        self._objects_head = {}
        self._links_head = {}

        """ see model/model.xml """
        """ 1) class """
        self._packagename = 'com.informatica.ldm.odata'
        self._model_endpoint = 'com.informatica.ldm.odata.endpoint'
        #self._model_entitytype = 'com.informatica.ldm.odata.entitytype'
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
        self._association_entitysetproperty = 'com.informatica.ldm.odata.entitysetproperty'

        """ 4) data type mapping """

        self._datatype = {
            'Edm.Int16': 'INT',
            'Edm.Int64': 'INT',
            'Edm.Int32': 'INT',
            'Edm.Guid': 'VARCHAR',
            'Edm.String': 'VARCHAR',
            'Edm.Byte': 'VARCHAR',
            'Edm.SByte': 'VARCHAR',
            'Edm.Binary': 'BINARY',
            'Edm.DateTimeOffset': 'TIMESTAMP',
            'Edm.Decimal': 'DECIMAL',
            'Edm.Single': 'DECIMAL',
            'Edm.Time': 'TIMESTAMP',
            'Edm.DateTime': 'TIMESTAMP',
            'Edm.Double': 'DECIMAL',
            'Edm.Boolean': 'BIT',
        }

    def print_out_metadata_info(self):
        print('... wrong calling base function ...')
    
    def convert_objects(self, force=False):
        """ create objects.csv """
        import os
        assert os.path.exists(self._dir), "given dir not found: " + str(self._dir)

        if not force:
            assert os.path.exists(self._dir + '/links.csv') == False, "links.csv exists in: " + str(self._dir)
            assert os.path.exists(self._dir + '/objects.csv') == False, "objects.csv exists in: " + str(self._dir)

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

    def fetch_pdata(self, force=False):
        print('... wrong calling base function ...')
    
    def invoke_worker(self):
        from multiprocessing import Process
        from subprocess import Popen, PIPE
        import main

        def exec_(i):
            p = Popen(['./main.py', '--asworker', '-f', '--asworker_id=' + str(i)])
            stdout, stderr = p.communicate()
  
        from concurrent.futures import ThreadPoolExecutor
        import time

        with ThreadPoolExecutor(max_workers=int(self._worker)) as executor:
            start = time.time()
            futures = executor.map(exec_, range(int(self._worker)))
            for future in futures:
                pass
            end = time.time()
            print(f'[... PROFILING FINISHED IN {end-start} SECONDS ...]')

            """ start consumer process """
            import os
            import csv

            if os.path.exists(self._dir):
                with open(self._dir + f'/{self._resource}_DatasetMapping.csv', 'w') as f:
                    print(f'[... CREATETING {self._dir}/{self._resource}_DatasetMapping.csv ...]')
                    writer = csv.writer(f, delimiter=',')
                    writer.writerow(['DatasetId','AssociationType','DataTypeAttribute','DatasetFilePath'])
                    p = Popen(['./main.py', '--asconsumer', '-f'])
                    stdout, stderr = p.communicate()

        """
        for i in range(int(self._worker)):
            args = ['./main.py', '--asworker', '-f', '--asworker_id=' + str(i+1)]
            p = Process(target=main._main, args=(args,))
            p.start()
        """

    def profile(self, esname):
        import os
        print(f'[... (WORKER {self._workerid} - PID {os.getpid()}) START PROFILING JOB: {esname}]')
    
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
                print(f'[... CREATETING {self._dir}/ProfileableClassTypes.csv ...]')
                f.write('com.informatica.ldm.odata.property')
                f.close()

        else:
            print('[... NO DIR FOR PROFILING IS GIVEN...]')
    
    def prepare_dataset_mapping(self, obj):
        print(obj)
        

