""" OData Interface """
""" V2 implemented via https://github.com/SAP/python-pyodata """
""" TODO: V4 open """

class ODataConverter():
    def __init__(self, endpoint, dir, worker, worker_id=0):
        self._endpoint = endpoint
        self._dir = dir
        self._worker = worker
        self._workerid = worker_id

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
        self._model_property_primarykey = 'com.informatica.ldm.odata.PrimaryKeyColumn'
        self._model_property_datatype = 'com.informatica.ldm.odata.DATATYPE'
        self._model_property_odataversion = 'com.informatica.ldm.odata.ODATAVERSION'
        self._model_property_nullable = 'com.informatica.ldm.odata.NULLABLE'
        self._model_property_maxlength = 'com.informatica.ldm.odata.MAXLENGTH'
        self._model_property_fixedlength = 'com.informatica.ldm.odata.FIXEDLENGTH'
        self._model_property_precision = 'com.informatica.ldm.odata.PRECISION'
        self._model_property_scale = 'com.informatica.ldm.odata.SCALE'

        """ 3) association """
        self._association_endpointentityset = 'com.informatica.ldm.odata.endpointentityset'
        self._association_entitysetproperty = 'com.informatica.ldm.odata.entitysetproperty'

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
        import main

        for i in range(int(self._worker)):
            args = ['./main.py', '--asworker', '-f', '--asworker_id=' + str(i+1)]
            p = Process(target=main._main, args=(args,))
            p.start()

    def profile(self, esname):
        import os
        print(f'[... (WORKER {self._workerid} - PID {os.getpid()}) START PROFILING JOB: {esname}]')
