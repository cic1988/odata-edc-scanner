""" OData V4 """

from .odata_converter import ODataConverter
from taskqueue.taskqueue import consumer, worker, get_taskqueue, get_consumerqueue
from .odata.service import ODataService

import logging

logger = logging.getLogger(__name__)

class ODataConverterV4(ODataConverter):
    def __init__(self, endpoint, dir, resource, worker, worker_id=0):
        ODataConverter.__init__(self, endpoint, dir, resource, worker, worker_id)
        self._service = ODataService(endpoint, reflect_entities=True)

        meta = self._service.metadata
        document = meta.load_document()
        schemas, entity_sets, actions, functions = meta.parse_document(document)
        self._entities = entity_sets
        self._modelname = schemas[0]['name']

    def print_out_metadata_info(self):

        """
        TODO: implement for v4 open
        """

        return

    def convert_objects(self, force=False):
        super().convert_objects(force)

        """ create tmp file """
        import csv
        import uuid
        import os
        import copy
        import re

        tmp_file = os.path.abspath(self._dir) + '/objects.csv.' + uuid.uuid4().hex.upper()[0:6]

        with open(tmp_file, 'w', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, self._objects_head.keys())
            writer.writeheader()

            """ 1) endpoint """

            endpoint = copy.deepcopy(self._objects_head)
            endpoint['class'] = self._model_endpoint
            endpoint['identity'] = 'Endpoint'
            endpoint['core.name'] = 'Endpoint'
            endpoint['core.description'] = 'Endpoint'
            endpoint[self._model_property_datatype] = 'URL'
            endpoint[self._model_property_odataversion] = '4'

            writer.writerow(endpoint)

            for es in self._entities.values():

                entitytype_name = es['type'].replace(self._modelname + '.', "") 

                """ 2) entitytype """

                entitytype = copy.deepcopy(self._objects_head)
                entitytype['class'] = self._model_entitytype
                entitytype['identity'] = endpoint['identity'] + '/' + entitytype_name
                entitytype['core.name'] = entitytype_name
                entitytype['core.description'] = entitytype_name
                entitytype[self._model_entityset_entitytype] = entitytype_name
                entitytype[self._model_property_odataversion] = '4'
                writer.writerow(entitytype)

                """ 3) properties """

                properties = es['schema']['properties']

                for prop in properties:
                    column = copy.deepcopy(self._objects_head)
                    column['class'] = self._model_property
                    column['identity'] =  entitytype['identity'] + '/' + prop["name"]
                    column['core.name'] = prop["name"]
                    column['core.description'] = prop["name"]
                    column[self._model_property_datatype] = prop["type"]
                    column[self._model_property_odataversion] = '4'

                    """
                    TODO: these five attributes are currently not implemented in the odata_v4 module
                    """
                    column[self._model_property_nullable] = getattr(prop, 'nullable', None)
                    column[self._model_property_maxlength] = getattr(prop, 'max_length', None)
                    column[self._model_property_fixedlength] = getattr(prop, 'fixed_length', None)
                    column[self._model_property_precision] = None if getattr(prop, 'precision', None) == 0 else getattr(prop, 'precision', None)
                    column[self._model_property_scale] = None if getattr(prop, 'scale', None) == 0 else getattr(prop, 'scale', None)

                    """ 4) key """

                    if prop['is_primary_key']:
                        column['class'] = self._model_key
                        column[self._model_property_primarykey] = True

                    writer.writerow(column)

                """ 5) navigation properties """
                
                navigation_properties = es['schema']['navigation_properties']

                for prop in navigation_properties:
                    res = re.search(f"Collection\({self._modelname}.([a-zA-Z0-9]+)\)", prop["type"])

                    if not res:
                        continue

                    column = copy.deepcopy(self._objects_head)
                    column['class'] = self._model_navigationproperty
                    column['identity'] =  entitytype['identity'] + '/' + prop["name"]
                    column['core.name'] = prop["name"]
                    column['core.description'] = prop["name"]
                    column[self._model_property_datatype] = res.group(1)
                    column[self._model_property_odataversion] = '4'

                    writer.writerow(column)

        if os.path.exists(tmp_file):
            os.rename(tmp_file, os.path.abspath(self._dir) + '/objects.csv')

    def convert_links(self, force=False):
        super().convert_links(force)

        """ create tmp file """
        import csv
        import uuid
        import os
        import copy
        import re

        tmp_file = os.path.abspath(self._dir) + '/links.csv.' + uuid.uuid4().hex.upper()[0:6]

        with open(tmp_file, 'w', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, self._links_head.keys())
            writer.writeheader()
             
            for es in self._entities.values():

                entitytype_name = es['type'].replace(self._modelname + '.', "") 

                """ 0) endpoint to entitytype """
                toplevel = copy.deepcopy(self._links_head)
                toplevel['association'] = self._association_resourceparanchild
                toplevel['toObjectIdentity'] = 'Endpoint/' + entitytype_name
    
                """ 1) com.informatica.ldm.odata.endpointentitytype """

                endpointentitytype = copy.deepcopy(self._links_head)
                endpointentitytype['association'] = self._association_entitytypeproperty
                endpointentitytype['fromObjectIdentity'] = 'Endpoint'
                endpointentitytype['toObjectIdentity'] = 'Endpoint/' + entitytype_name

                writer.writerow(endpointentitytype)

                """ 2) com.informatica.ldm.odata.entitytypeproperty """

                properties = es['schema']['properties']

                for prop in properties:
                    entitytypeproperty = copy.deepcopy(self._links_head)
                    entitytypeproperty['association'] = self._association_entitytypeproperty
                    entitytypeproperty['fromObjectIdentity'] = endpointentitytype['toObjectIdentity']
                    entitytypeproperty['toObjectIdentity'] = endpointentitytype['toObjectIdentity'] + '/' + prop['name']

                    """ 3) key """

                    if prop['is_primary_key']:
                        entitytypeproperty['association'] = self._association_entitytypekey

                    writer.writerow(entitytypeproperty)
                
                """ 4) com.informatica.ldm.odata.entitytypenavigationproperty """

                navigation_properties = es['schema']['navigation_properties']

                for prop in navigation_properties:
                    res = re.search(f"Collection\({self._modelname}.([a-zA-Z0-9]+)\)", prop["type"])

                    if not res:
                        continue

                    entitytypenavigationproperty = copy.deepcopy(self._links_head)
                    entitytypenavigationproperty['association'] = self._association_entitytypenavigationproperty
                    entitytypenavigationproperty['fromObjectIdentity'] = endpointentitytype['toObjectIdentity']
                    entitytypenavigationproperty['toObjectIdentity'] = endpointentitytype['toObjectIdentity'] + '/' + prop['name']

                    writer.writerow(entitytypenavigationproperty)
        
        if os.path.exists(tmp_file):
            os.rename(tmp_file, os.path.abspath(self._dir) + '/links.csv')

    def fetch_pdata(self, force=False):
        profiling_q = get_taskqueue('profiling')
        consumer_q = get_consumerqueue('profiling')
        profiling_q.clear()
        consumer_q.clear()
             
        for es in self._entities:
            profiling_q.put(es)

        super().invoke_worker()

    @worker('profiling', timeout=5)
    def profile(self, esname):
        import csv

        if esname:
            super().profile(esname)
            query = self._service.query(self._service.entities[esname])
            entities = query.limit(self._profling_lines)
            entitytype_name = self._entities[esname]['type'].replace(self._modelname + '.', "") 

            """ only profile when entities """

            properties = self._entities[esname]['schema']['properties']
            
            header = []
            for prop in properties:
                header.append(prop['name'])
                
            if header:
                with open(self._dir + '/' + esname + '.csv', 'w', encoding='UTF8') as f:
                    writer = csv.writer(f)
                    writer.writerow(header)

                    for entity in entities:
                        row = []

                        for prop in properties:
                            """ TODO: Edm.Binary is not allowed to profile """
                            if prop['type'] == 'Edm.Binary':
                                row.append(None)
                                continue
                            row.append(str(getattr(entity, prop['name'])))
                        writer.writerow(row)
            
            """ put dataset identifier into consumer queue """
            get_consumerqueue('profiling').put({
                'id': 'Endpoint/' + entitytype_name,
                'file': self._dir + '/' + esname + '.csv'
            })
    
    @consumer('profiling', timeout=5)
    def prepare_dataset_mapping(self, obj):
        import os
        import csv

        if os.path.exists(self._dir):
            with open(self._dir + f'/{self._resource}_DatasetMapping.csv', 'a') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow([
                    obj['id'],
                    self._association_entitytypeproperty,
                    self._model_property_datatype,
                    os.path.abspath(obj['file'])])
