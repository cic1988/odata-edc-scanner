""" OData V2 """

from .odata_converter import ODataConverter
from taskqueue.taskqueue import consumer, worker, get_taskqueue, get_consumerqueue
from pyodata.v2.model import EntityType

import requests
import pyodata
import logging

logger = logging.getLogger(__name__)

class ODataConverterV2(ODataConverter):
    def __init__(self, params):
        # see: https://github.com/SAP/python-pyodata/pull/149
        pyodata.v2.model.FIX_SCREWED_UP_MINIMAL_DATETIME_VALUE = True
        ODataConverter.__init__(self, params['root_url'], params['dir'], params['resource'], params['worker'], params['worker_id'])
        self._session = requests.Session()
        self._session.auth = (params['username'], params['password'])
        self._client = pyodata.Client(params['root_url'], self._session)

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

            for et in self._client.schema.entity_types:

                if not isinstance(et, EntityType):
                    continue

                """ 2) entitytype """

                entitytype = copy.deepcopy(self._objects_head)
                entitytype['class'] = self._model_entitytype
                entitytype['identity'] = endpoint['identity'] + '/' + et.name
                entitytype['core.name'] = et.name
                entitytype['core.description'] = et.name
                entitytype[self._model_entityset_entitytype] = et.name
                entitytype[self._model_property_odataversion] = '2'
                writer.writerow(entitytype)

                """ 3) (primary) key """

                for prop in et.key_proprties:
                    primarykey = copy.deepcopy(self._objects_head)
                    primarykey['class'] = self._model_key
                    primarykey['identity'] =  entitytype['identity'] + '/' + prop.name
                    primarykey['core.name'] = prop.name
                    #primarykey['core.description'] = vars(prop)
                    primarykey['core.description'] = ''
                    primarykey[self._model_property_primarykey] = True
                    primarykey[self._model_property_datatype] = prop.typ.name
                    primarykey[self._model_property_odataversion] = '2'
                    primarykey[self._model_property_nullable] = getattr(prop, 'nullable', None)
                    primarykey[self._model_property_maxlength] = getattr(prop, 'max_length', None)
                    primarykey[self._model_property_fixedlength] = getattr(prop, 'fixed_length', None)
                    primarykey[self._model_property_precision] = None if getattr(prop, 'precision', None) == 0 else getattr(prop, 'precision', None)
                    primarykey[self._model_property_scale] = None if getattr(prop, 'scale', None) == 0 else getattr(prop, 'scale', None)

                    writer.writerow(primarykey)
                
                """ 4) column """

                for prop in et.proprties():
                    column = copy.deepcopy(self._objects_head)
                    column['class'] = self._model_property
                    column['identity'] =  entitytype['identity'] + '/' + prop.name
                    column['core.name'] = prop.name
                    #column['core.description'] = vars(prop)
                    column['core.description'] = ''
                    column[self._model_property_datatype] = prop.typ.name
                    column[self._model_property_odataversion] = '2'
                    column[self._model_property_nullable] = getattr(prop, 'nullable', None)
                    column[self._model_property_maxlength] = getattr(prop, 'max_length', None)
                    column[self._model_property_fixedlength] = getattr(prop, 'fixed_length', None)
                    column[self._model_property_precision] = None if getattr(prop, 'precision', None) == 0 else getattr(prop, 'precision', None)
                    column[self._model_property_scale] = None if getattr(prop, 'scale', None) == 0 else getattr(prop, 'scale', None)

                    writer.writerow(column)
                
                """ 5) navigation property """
                for prop in et.nav_proprties:
                    navigation = copy.deepcopy(self._objects_head)
                    navigation['class'] = self._model_navigationproperty
                    navigation['identity'] =  entitytype['identity'] + '/' + prop.name
                    navigation['core.name'] = prop.name
                    #navigation['core.description'] = vars(prop)
                    navigation['core.description'] = ''
                    navigation[self._model_property_datatype] = prop.typ.name
                    navigation[self._model_property_odataversion] = '2'
                    navigation[self._model_property_nullable] = getattr(prop, 'nullable', None)
                    navigation[self._model_property_maxlength] = getattr(prop, 'max_length', None)
                    navigation[self._model_property_fixedlength] = getattr(prop, 'fixed_length', None)
                    navigation[self._model_property_precision] = None if getattr(prop, 'precision', None) == 0 else getattr(prop, 'precision', None)
                    navigation[self._model_property_scale] = None if getattr(prop, 'scale', None) == 0 else getattr(prop, 'scale', None)

                    writer.writerow(navigation)

            """ 6) function import """
            for fs in self._client.schema.function_imports:
                functionimport = copy.deepcopy(self._objects_head)
                functionimport['class'] = self._model_functionimport
                functionimport['identity'] = endpoint['identity'] + '/' + fs.name
                functionimport['core.name'] = fs.name
                functionimport['core.description'] = f'{fs.http_method} {fs.name}'
                functionimport[self._model_property_odataversion] = '2'

                if fs.return_type is not None:
                    functionimport['core.description'] = functionimport['core.description'] + f' < {fs.return_type.name}'

                writer.writerow(functionimport)

                for param in fs.parameters:
                    parameter = copy.deepcopy(self._objects_head)
                    parameter['class'] = self._model_parameter
                    parameter['identity'] =  functionimport['identity'] + '/' + param.name
                    parameter['core.name'] = param.name
                    parameter['core.description'] = f' > {param.name}({param.typ.name})'
                    parameter[self._model_property_datatype] = param.typ.name
                    parameter[self._model_property_odataversion] = '2'
                    parameter[self._model_property_nullable] = getattr(param, 'nullable', None)
                    parameter[self._model_property_maxlength] = getattr(param, 'max_length', None)
                    parameter[self._model_property_fixedlength] = getattr(param, 'fixed_length', None)
                    parameter[self._model_property_precision] = None if getattr(param, 'precision', None) == 0 else getattr(param, 'precision', None)
                    parameter[self._model_property_scale] = None if getattr(param, 'scale', None) == 0 else getattr(param, 'scale', None)

                    writer.writerow(parameter)

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

            for et in self._client.schema.entity_types:

                if not isinstance(et, EntityType):
                    continue

                """ 0) endpoint to entitytype """
                toplevel = copy.deepcopy(self._links_head)
                toplevel['association'] = self._association_resourceparanchild
                toplevel['toObjectIdentity'] = 'Endpoint/' + et.name

                writer.writerow(toplevel)
    
                """ 1) com.informatica.ldm.odata.endpointentitytype """
                endpointentitytype = copy.deepcopy(self._links_head)
                endpointentitytype['association'] = self._association_endpointentitytype
                endpointentitytype['fromObjectIdentity'] = 'Endpoint'
                endpointentitytype['toObjectIdentity'] = 'Endpoint/' + et.name

                writer.writerow(endpointentitytype)

                """ 2) com.informatica.ldm.odata.entitytypeproperty (primarykey) """

                for prop in et.key_proprties:
                    entitytypeproperty = copy.deepcopy(self._links_head)
                    entitytypeproperty['association'] = self._association_entitytypeproperty
                    entitytypeproperty['fromObjectIdentity'] = endpointentitytype['toObjectIdentity']
                    entitytypeproperty['toObjectIdentity'] = endpointentitytype['toObjectIdentity'] + '/' + prop.name

                    writer.writerow(entitytypeproperty)
                
                """ 3) com.informatica.ldm.odata.entitytypeproperty """

                for prop in et.proprties():
                    entitytypeproperty = copy.deepcopy(self._links_head)
                    entitytypeproperty['association'] = self._association_entitytypeproperty
                    entitytypeproperty['fromObjectIdentity'] = endpointentitytype['toObjectIdentity']
                    entitytypeproperty['toObjectIdentity'] = endpointentitytype['toObjectIdentity'] + '/' + prop.name

                    writer.writerow(entitytypeproperty)
                
                """ 4) com.informatica.ldm.odata.entitytynavigationpeproperty """

                for prop in et.nav_proprties:
                    entitytynavigationpeproperty = copy.deepcopy(self._links_head)
                    entitytynavigationpeproperty['association'] = self._association_entitytypeproperty
                    entitytynavigationpeproperty['fromObjectIdentity'] = endpointentitytype['toObjectIdentity']
                    entitytynavigationpeproperty['toObjectIdentity'] = endpointentitytype['toObjectIdentity'] + '/' + prop.name

                    writer.writerow(entitytynavigationpeproperty)

                    """ lineage """

                    if prop.association:

                        """
                        TODO: the order shows the data flow direction:

                        Employee -> Territory

                        <Association Name="EmployeeTerritories">
                            <End Role="Employees" Type="NorthwindModel.Employee" Multiplicity="*"/>
                            <End Role="Territories" Type="NorthwindModel.Territory" Multiplicity="*"/>
                        </Association>

                        """
                        if len(prop.association.end_roles) != 2:
                            continue
                        
                        entitytype_from = prop.association.end_roles[0].entity_type.name
                        entitytype_to = prop.association.end_roles[1].entity_type.name

                        """ 5) core.DataSetDataFlow """
                        
                        entitytypefromto = copy.deepcopy(self._links_head)
                        entitytypefromto['association'] = self._association_datasetdataflow
                        entitytypefromto['fromObjectIdentity'] = 'Endpoint/' + entitytype_from
                        entitytypefromto['toObjectIdentity'] = 'Endpoint/' + entitytype_to

                        writer.writerow(entitytypefromto)

                        """
                        TODO: multiple order shows the property flow. Following cases supported:
                              1) 1-n
                              2) n-1
                              3) n-n

                        However, n-m (n is not m) is not allowed. Case not seen yet.

                        <ReferentialConstraint>
                            <Principal Role="FromRole_ContactOriginDataAdditionalID">
                                <PropertyRef Name="ContactOrigin"/>
                                <PropertyRef Name="ContactID"/>
                            </Principal>
                            <Dependent Role="ToRole_ContactOriginDataAdditionalID">
                                <PropertyRef Name="ContactOrigin"/>
                                <PropertyRef Name="ContactID"/>
                            </Dependent>
                        </ReferentialConstraint>
                        """

                        if prop.association.referential_constraint:

                            """ 6) core.DirectionalDataFlow (property -> property) """

                            if len(prop.association.referential_constraint.principal.property_names) == 1 or len(prop.association.referential_constraint.dependent.property_names) == 1:
                                for prop_from in prop.association.referential_constraint.principal.property_names:
                                    for prop_to in prop.association.referential_constraint.dependent.property_names:
                                        propertyfromto = copy.deepcopy(self._links_head)
                                        propertyfromto['association'] = self._association_directionaldataflow
                                        propertyfromto['fromObjectIdentity'] = entitytypefromto['fromObjectIdentity'] + '/' + prop_from
                                        propertyfromto['toObjectIdentity'] = entitytypefromto['toObjectIdentity'] + '/' + prop_to

                                        writer.writerow(propertyfromto)
                            elif len(prop.association.referential_constraint.principal.property_names) == len(prop.association.referential_constraint.dependent.property_names):
                                for idx, prop_from in enumerate(prop.association.referential_constraint.principal.property_names):
                                    prop_to = prop.association.referential_constraint.dependent.property_names[idx]
                                    propertyfromto = copy.deepcopy(self._links_head)
                                    propertyfromto['association'] = self._association_directionaldataflow
                                    propertyfromto['fromObjectIdentity'] = entitytypefromto['fromObjectIdentity'] + '/' + prop_from
                                    propertyfromto['toObjectIdentity'] = entitytypefromto['toObjectIdentity'] + '/' + prop_to

                                    writer.writerow(propertyfromto)
                            else:
                                logger.debug(f'[... n-m is not allowed: principle: {prop.association.referential_constraint.principal.property_name}]')
                                logger.debug(f'[... n-m is not allowed: dependent: {prop.association.referential_constraint.dependent.property_name}]')
                        else:

                            """ 6) core.DirectionalDataFlow (navigationproperty -> navigationproperty) """

                            navigationpropertyentitytype = copy.deepcopy(self._links_head)
                            navigationpropertyentitytype['association'] = self._association_directionaldataflow

                            navigation2propertyentitytype = entitytype_to if et.name == entitytype_from else entitytype_from

                            for et_2nd in self._client.schema.entity_types:
                                if et_2nd.name == navigation2propertyentitytype:
                                    navigation2propertyentitytype = et_2nd
                                    break

                            navigation2property = None
                            
                            for navigation2property_i in navigation2propertyentitytype.nav_proprties:
                                if navigation2property_i.association.name == prop.association.name:
                                    navigation2property = navigation2property_i
                            
                            if navigation2property:

                                if et.name == entitytype_from:
                                    navigationpropertyentitytype['fromObjectIdentity'] = entitytypefromto['fromObjectIdentity'] + '/' + prop.name
                                    navigationpropertyentitytype['toObjectIdentity'] = entitytypefromto['toObjectIdentity'] + '/' + navigation2property.name
                                else:
                                    navigationpropertyentitytype['fromObjectIdentity'] = entitytypefromto['fromObjectIdentity'] + '/' + navigation2property.name
                                    navigationpropertyentitytype['toObjectIdentity'] = entitytypefromto['toObjectIdentity'] + '/' + prop.name

                                writer.writerow(navigationpropertyentitytype)
            
            for fs in self._client.schema.function_imports:
                """ 7) endpoint to function import """
                toplevel = copy.deepcopy(self._links_head)
                toplevel['association'] = self._association_resourceparanchild
                toplevel['toObjectIdentity'] = 'Endpoint/' + fs.name

                writer.writerow(toplevel)

                """ 8) com.informatica.ldm.odata.endpointfunctionimport """
                endpointfunctionimport = copy.deepcopy(self._links_head)
                endpointfunctionimport['association'] = self._association_endpointfunctionimport
                endpointfunctionimport['fromObjectIdentity'] = 'Endpoint'
                endpointfunctionimport['toObjectIdentity'] = 'Endpoint/' + fs.name

                writer.writerow(endpointfunctionimport)

                """ 8) com.informatica.ldm.odata.functionimportparameter """
                for param in fs.parameters:
                    functionimportparameter = copy.deepcopy(self._links_head)
                    functionimportparameter['association'] = self._association_functionimportparameter
                    functionimportparameter['fromObjectIdentity'] = endpointfunctionimport['toObjectIdentity']
                    functionimportparameter['toObjectIdentity'] = endpointfunctionimport['toObjectIdentity'] + '/' + param.name

                    writer.writerow(functionimportparameter)

        
        if os.path.exists(tmp_file):
            os.rename(tmp_file, os.path.abspath(self._dir) + '/links.csv')

    def fetch_pdata(self, force=False):
        profiling_q = get_taskqueue('profiling')
        consumer_q = get_consumerqueue('profiling')
        profiling_q.clear()
        consumer_q.clear()

        for es in self._client.schema.entity_sets:
            if self._profiling_filter and len(self._profiling_filter) > 0:
                if es.name not in self._profiling_filter:
                    continue
            profiling_q.put(es.name)

        super().invoke_worker()

    @worker('profiling', timeout=5)
    def profile(self, esname):
        import csv

        if esname:
            super().profile(esname)

            entitysets = getattr(self._client.entity_sets, esname, None)

            if not entitysets:
                return
            
            entities = None
            
            try:
                count = 0

                if self._profling_lines != 'all':
                    count = self._profling_lines
                else:
                    count = entitysets.get_entities().count().execute()
                """
                TODO: pagination can be improved ...
                """
                skip = 0
                count = int(count)
                top = 10000 if count > 10000 else count
                entities = []

                while skip < count:
                    entities = entities + entitysets.get_entities().skip(skip).top(top).execute()
                    skip += top

            except BaseException as err:
                logger.info(f'[... PROFILING ERROR at {esname} : {err} ...]')
                return

            if not entities:
                logger.info(f'[... PROFILING ({esname}) IGNORED : empty entity ...]')
                return

            """ only profile when entities """
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
                    self._association_entitytypeproperty,
                    self._model_property_datatype,
                    os.path.abspath(obj['file'])])
