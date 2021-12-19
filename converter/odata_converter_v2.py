""" OData V2 """

from .odata_converter import ODataConverter
from taskqueue.taskqueue import consumer, worker, get_taskqueue, get_consumerqueue

import requests
import pyodata
import logging

logger = logging.getLogger(__name__)

class ODataConverterV2(ODataConverter):
    def __init__(self, endpoint, dir, resource, worker, worker_id=0):
        ODataConverter.__init__(self, endpoint, dir, resource, worker, worker_id)
        self._session = requests.Session()
        self._client = pyodata.Client(endpoint, self._session)

    def print_out_metadata_info(self):

        for es in self._client.schema.entity_sets:
            logger.debug(es.name)

            proprties = es.entity_type.proprties()

            for prop in es.entity_type.key_proprties:
                logger.debug(f' K {prop.name}({prop.typ.name})')
                proprties.remove(prop)

            for prop in proprties:
                logger.debug(f' + {prop.name}({prop.typ.name})')

            for prop in es.entity_type.nav_proprties:
                logger.debug(f' + {prop.name}({prop.to_role.entity_type_name})')

        for fs in self._client.schema.function_imports:
            logger.debug(f'{fs.http_method} {fs.name}')

            for param in fs.parameters:
                logger.debug(f' > {param.name}({param.typ.name})')

            if fs.return_type is not None:
                logger.debug(f' < {fs.return_type.name}')

    def convert_objects(self, force=False):
        super().convert_objects(force)

        """ create tmp file """
        import csv
        import uuid
        import os
        import copy

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
            endpoint[self._model_property_odataversion] = '2'

            writer.writerow(endpoint)

            for es in self._client.schema.entity_sets:

                """ 2) entitytype """

                entitytype = copy.deepcopy(self._objects_head)
                entitytype['class'] = self._model_entitytype
                entitytype['identity'] = endpoint['identity'] + '/' + es.entity_type.name
                entitytype['core.name'] = es.entity_type.name
                entitytype['core.description'] = es.entity_type.name
                entitytype[self._model_entityset_entitytype] = es.entity_type.name
                entitytype[self._model_property_odataversion] = '2'
                writer.writerow(entitytype)

                """ 3) (primary) key """

                proprties = es.entity_type.proprties()

                for prop in es.entity_type.key_proprties:
                    primarykey = copy.deepcopy(self._objects_head)
                    primarykey['class'] = self._model_property
                    primarykey['identity'] =  entitytype['identity'] + '/' + prop.name
                    primarykey['core.name'] = prop.name
                    primarykey['core.description'] = prop.name
                    primarykey[self._model_property_primarykey] = True
                    primarykey[self._model_property_datatype] = prop.typ.name
                    primarykey[self._model_property_odataversion] = '2'
                    primarykey[self._model_property_nullable] = getattr(prop, 'nullable', None)
                    primarykey[self._model_property_maxlength] = getattr(prop, 'max_length', None)
                    primarykey[self._model_property_fixedlength] = getattr(prop, 'fixed_length', None)
                    primarykey[self._model_property_precision] = None if getattr(prop, 'precision', None) == 0 else getattr(prop, 'precision', None)
                    primarykey[self._model_property_scale] = None if getattr(prop, 'scale', None) == 0 else getattr(prop, 'scale', None)

                    writer.writerow(primarykey)
                    proprties.remove(prop)
                
                """ 4) column """

                for prop in proprties:
                    column = copy.deepcopy(self._objects_head)
                    column['class'] = self._model_property
                    column['identity'] =  entitytype['identity'] + '/' + prop.name
                    column['core.name'] = prop.name
                    column['core.description'] = prop.name
                    column[self._model_property_datatype] = prop.typ.name
                    column[self._model_property_odataversion] = '2'
                    column[self._model_property_nullable] = getattr(prop, 'nullable', None)
                    column[self._model_property_maxlength] = getattr(prop, 'max_length', None)
                    column[self._model_property_fixedlength] = getattr(prop, 'fixed_length', None)
                    column[self._model_property_precision] = None if getattr(prop, 'precision', None) == 0 else getattr(prop, 'precision', None)
                    column[self._model_property_scale] = None if getattr(prop, 'scale', None) == 0 else getattr(prop, 'scale', None)

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

        tmp_file = os.path.abspath(self._dir) + '/links.csv.' + uuid.uuid4().hex.upper()[0:6]

        with open(tmp_file, 'w', encoding='UTF8', newline='') as f:
             writer = csv.DictWriter(f, self._links_head.keys())
             writer.writeheader()

             for es in self._client.schema.entity_sets:
    
                """ 1) com.informatica.ldm.odata.endpointentitytype """
                endpointentitytype = copy.deepcopy(self._links_head)
                endpointentitytype['association'] = self._association_entitysetproperty
                endpointentitytype['fromObjectIdentity'] = 'Endpoint'
                endpointentitytype['toObjectIdentity'] = 'Endpoint/' + es.entity_type.name

                writer.writerow(endpointentitytype)

                """ 2) com.informatica.ldm.odata.entitytypeproperty (primarykey) """

                proprties = es.entity_type.proprties()

                for prop in es.entity_type.key_proprties:
                    primarykeyproperty = copy.deepcopy(self._links_head)
                    primarykeyproperty['association'] = self._association_entitytypeproperty
                    primarykeyproperty['fromObjectIdentity'] = endpointentitytype['toObjectIdentity']
                    primarykeyproperty['toObjectIdentity'] = endpointentitytype['toObjectIdentity'] + '/' + prop.name

                    writer.writerow(primarykeyproperty)
                    proprties.remove(prop)
                
                """ 3) com.informatica.ldm.odata.entitysetproperty """

                for prop in proprties:
                    entitysetproperty = copy.deepcopy(self._links_head)
                    entitysetproperty['association'] = self._association_entitytypeproperty
                    entitysetproperty['fromObjectIdentity'] = endpointentitytype['toObjectIdentity']
                    entitysetproperty['toObjectIdentity'] = endpointentitytype['toObjectIdentity'] + '/' + prop.name

                    writer.writerow(entitysetproperty)
        
        if os.path.exists(tmp_file):
            os.rename(tmp_file, os.path.abspath(self._dir) + '/links.csv')

    def fetch_pdata(self, force=False):
        profiling_q = get_taskqueue('profiling')
        consumer_q = get_consumerqueue('profiling')
        profiling_q.clear()
        consumer_q.clear()

        for es in self._client.schema.entity_sets:
            profiling_q.put(es.name)

        super().invoke_worker()

    @worker('profiling', timeout=5)
    def profile(self, esname):
        import csv

        if esname:
            super().profile(esname)
            entities = self._client.entity_sets._entity_sets[esname].get_entities().top(self._profling_lines).execute()
            et = None

            """ only profile when entities """
            if entities:
                entity = entities[0]
                et = entity._entity_type.name
                proprties = entity._entity_type.proprties()

                header = []
                for prop in proprties:
                    header.append(prop.name)
                
                if header:
                    with open(self._dir + '/' + esname + '.csv', 'w', encoding='UTF8') as f:
                        writer = csv.writer(f)
                        writer.writerow(header)

                        for entity in entities:
                            row = []

                            for prop in proprties:
                                """ TODO: Edm.Binary is not allowed to profile """
                                if prop.typ.name == 'Edm.Binary':
                                    row.append(None)
                                    continue
                                row.append(str(getattr(entity, prop.name)))
                            writer.writerow(row)
            
            """ put dataset identifier into consumer queue """

            if et:
                get_consumerqueue('profiling').put({
                    'id': 'Endpoint/' + et,
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
                    self._association_entitysetproperty,
                    self._model_property_datatype,
                    os.path.abspath(obj['file'])])
