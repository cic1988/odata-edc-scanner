""" OData V4 """

from .odata_converter import ODataConverter
from taskqueue.taskqueue import consumer, worker, get_taskqueue, get_consumerqueue
from .odata.service import ODataService

class ODataConverterV4(ODataConverter):
    def __init__(self, endpoint, dir, resource, worker, worker_id=0):
        ODataConverter.__init__(self, endpoint, dir, resource, worker, worker_id)
        self._service = ODataService(endpoint, reflect_entities=True)

        meta = self._service.metadata
        document = meta.load_document()
        schemas, entity_sets, actions, functions = meta.parse_document(document)
        self._entities = entity_sets

    def print_out_metadata_info(self):

        return

        for es in self._client.schema.entity_sets:
            print(es.name)

            proprties = es.entity_type.proprties()

            for prop in es.entity_type.key_proprties:
                print(f' K {prop.name}({prop.typ.name})')
                proprties.remove(prop)

            for prop in proprties:
                print(f' + {prop.name}({prop.typ.name})')

            for prop in es.entity_type.nav_proprties:
                print(f' + {prop.name}({prop.to_role.entity_type_name})')

        for fs in self._client.schema.function_imports:
            print(f'{fs.http_method} {fs.name}')

            for param in fs.parameters:
                print(f' > {param.name}({param.typ.name})')

            if fs.return_type is not None:
                print(f' < {fs.return_type.name}')

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
            endpoint[self._model_property_odataversion] = '4'

            writer.writerow(endpoint)

            for es in self._entities:

                """ 2) entityset """

                entityset = copy.deepcopy(self._objects_head)
                entityset['class'] = self._model_entityset
                entityset['identity'] = endpoint['identity'] + '/' + es
                entityset['core.name'] = es
                entityset['core.description'] = es
                entityset[self._model_entityset_entitytype] = es
                entityset[self._model_property_odataversion] = '4'
                writer.writerow(entityset)

                """ 3) properties """

                properties = self._entities[es]['schema']['properties']

                for prop in properties:
                    column = copy.deepcopy(self._objects_head)
                    column['class'] = self._model_property
                    column['identity'] =  entityset['identity'] + '/' + prop["name"]
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

                    if prop['is_primary_key']:
                        column[self._model_property_primarykey] = True

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
             
            for es in self._entities:
    
                """ 1) com.informatica.ldm.odata.endpointentityset """

                endpointentityset = copy.deepcopy(self._links_head)
                endpointentityset['association'] = self._association_endpointentityset
                endpointentityset['fromObjectIdentity'] = 'Endpoint'
                endpointentityset['toObjectIdentity'] = 'Endpoint/' + es

                writer.writerow(endpointentityset)

                """ 2) com.informatica.ldm.odata.entitysetproperty """

                properties = self._entities[es]['schema']['properties']

                for prop in properties:
                    entitysetproperty = copy.deepcopy(self._links_head)
                    entitysetproperty['association'] = self._association_entitysetproperty
                    entitysetproperty['fromObjectIdentity'] = endpointentityset['toObjectIdentity']
                    entitysetproperty['toObjectIdentity'] = endpointentityset['toObjectIdentity'] + '/' + prop['name']

                    writer.writerow(entitysetproperty)
        
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
                'id': 'Endpoint/' + esname,
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
