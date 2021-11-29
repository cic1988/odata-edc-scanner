""" OData V2 """

from converter.odata_converter import ODataConverter
from requests.sessions import session

import requests
import pyodata

class ODataConverterV2(ODataConverter):
    def __init__(self, endpoint):
        ODataConverter.__init__(self, endpoint)
        self._session = requests.Session()
        self._client = pyodata.Client(endpoint, self._session)

    def print_out_metadata_info(self):

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

    def convert_objects(self, dir, force=False):
        super().convert_objects(dir, force)

        """ create tmp file """
        import csv
        import uuid
        import os
        import copy

        tmp_file = os.path.abspath(dir) + '/objects.csv.' + uuid.uuid4().hex.upper()[0:6]

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

                """ 2) entityset """

                entityset = copy.deepcopy(self._objects_head)
                entityset['class'] = self._model_entityset
                entityset['identity'] = endpoint['identity'] + '/' + es.name
                entityset['core.name'] = es.name
                entityset['core.description'] = es.name
                entityset[self._model_property_odataversion] = '2'
                writer.writerow(entityset)

                """ 3) (primary) key """

                proprties = es.entity_type.proprties()

                for prop in es.entity_type.key_proprties:
                    primarykey = copy.deepcopy(self._objects_head)
                    primarykey['class'] = self._model_property
                    primarykey['identity'] =  entityset['identity'] + '/' + prop.name
                    primarykey['core.name'] = prop.name
                    primarykey['core.description'] = prop.name
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
                    column['identity'] =  entityset['identity'] + '/' + prop.name
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
            os.rename(tmp_file, os.path.abspath(dir) + '/objects.csv')

    def convert_links(self, dir, force=False):
        super().convert_links(dir, force)

        """ create tmp file """
        import csv
        import uuid
        import os
        import copy

        tmp_file = os.path.abspath(dir) + '/links.csv.' + uuid.uuid4().hex.upper()[0:6]

        with open(tmp_file, 'w', encoding='UTF8', newline='') as f:
             writer = csv.DictWriter(f, self._links_head.keys())
             writer.writeheader()

             for es in self._client.schema.entity_sets:
    
                """ 1) com.informatica.ldm.odata.endpointentityset """
                endpointentityset = copy.deepcopy(self._links_head)
                endpointentityset['association'] = self._association_endpointentityset
                endpointentityset['fromObjectIdentity'] = 'Endpoint'
                endpointentityset['toObjectIdentity'] = 'Endpoint/' + es.name

                writer.writerow(endpointentityset)

                """ 2) com.informatica.ldm.odata.entitysetproperty (primarykey) """

                proprties = es.entity_type.proprties()

                for prop in es.entity_type.key_proprties:
                    primarykeyproperty = copy.deepcopy(self._links_head)
                    primarykeyproperty['association'] = self._association_entitysetproperty
                    primarykeyproperty['fromObjectIdentity'] = endpointentityset['toObjectIdentity']
                    primarykeyproperty['toObjectIdentity'] = endpointentityset['toObjectIdentity'] + prop.name

                    writer.writerow(primarykeyproperty)
                    proprties.remove(prop)
                
                """ 3) com.informatica.ldm.odata.entitysetproperty """

                for prop in proprties:
                    entitysetproperty = copy.deepcopy(self._links_head)
                    entitysetproperty['association'] = self._association_entitysetproperty
                    entitysetproperty['fromObjectIdentity'] = endpointentityset['toObjectIdentity']
                    entitysetproperty['toObjectIdentity'] = endpointentityset['toObjectIdentity'] + '/' + prop.name

                    writer.writerow(entitysetproperty)
        
        if os.path.exists(tmp_file):
            os.rename(tmp_file, os.path.abspath(dir) + '/links.csv')

