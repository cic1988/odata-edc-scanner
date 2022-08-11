""" SAP Sales Cloud (OData V2) """

from .odata_converter import ODataConverter
from .odata_converter_v2 import ODataConverterV2
from .odata.service import ODataService
from taskqueue.taskqueue import consumer, worker, get_taskqueue, get_consumerqueue
from pyodata.v2.model import EntityType, Config, schema_from_xml
from pyodata.client import _fetch_metadata
from pyodata.exceptions import PyODataException, PyODataModelError, PyODataParserError

from lxml import etree

import io
import logging

logger = logging.getLogger(__name__)

class ODataMetabuilder():
    def __init__(self, session, params):
        url = params['root_url'].rstrip('/') + '/'
        self._xml = _fetch_metadata(session, url, logger)

        if isinstance(self._xml, str):
            mdf = io.StringIO(self._xml)
        elif isinstance(self._xml, bytes):
            mdf = io.BytesIO(self._xml)
        else:
            raise TypeError(f'Expected bytes or str type on metadata_xml, got : {type(self._xml)}')

        self._xml = mdf
        self._config = Config()
    
        """ XML elements """

        self._schema_nodes = []
        self._parents = {}

    def build(self):
        EDMX_WHITELIST = [
            'http://schemas.microsoft.com/ado/2007/06/edmx',
            'http://docs.oasis-open.org/odata/ns/edmx',
        ]

        EDM_WHITELIST = [
            'http://schemas.microsoft.com/ado/2006/04/edm',
            'http://schemas.microsoft.com/ado/2007/05/edm',
            'http://schemas.microsoft.com/ado/2008/09/edm',
            'http://schemas.microsoft.com/ado/2009/11/edm',
            'http://docs.oasis-open.org/odata/ns/edm'
        ]

        try:
            xml = etree.parse(self._xml)
        except etree.XMLSyntaxError as ex:
            raise PyODataParserError('Metadata document syntax error') from ex

        edmx = xml.getroot()

        namespaces = self._config.namespaces

        try:
            dataservices = next((child for child in edmx if etree.QName(child.tag).localname == 'DataServices'))
        except StopIteration:
            raise PyODataParserError('Metadata document is missing the element DataServices')

        try:
            schema = next((child for child in dataservices if etree.QName(child.tag).localname == 'Schema'))
        except StopIteration:
            raise PyODataParserError('Metadata document is missing the element Schema')
        
        if 'edmx' not in self._config.namespaces:
            namespace = etree.QName(edmx.tag).namespace

            if namespace not in EDMX_WHITELIST:
                raise PyODataParserError(f'Unsupported Edmx namespace - {namespace}')

            namespaces['edmx'] = namespace

        if 'edm' not in self._config.namespaces:
            namespace = etree.QName(schema.tag).namespace

            if namespace not in EDM_WHITELIST:
                raise PyODataParserError(f'Unsupported Schema namespace - {namespace}')

            namespaces['edm'] = namespace
        
        self._config.namespaces = namespaces

        self._schema_nodes = xml.xpath('/edmx:Edmx/edmx:DataServices/edm:Schema', namespaces=self._config.namespaces)
        
        for schema_node in self._schema_nodes:
            for entity_type in schema_node.xpath('edm:EntityType', namespaces=self._config.namespaces):
                try:
                    name = entity_type.get('Name')
                    parent = entity_type.get('{http://www.sap.com/Protocols/C4CData}parent-entity-type')

                    if parent:
                        self._parents[name] = parent
                except (KeyError, AttributeError) as ex:
                    logger.info(f'[... PARSER ERROR ({etree.tostring(entity_type)}) : {ex} ...]')

class SAPSCConverter(ODataConverterV2):

    def __init__(self, params):
        super().__init__(params)
        self._builder = ODataMetabuilder(self._session, params)
    
    def convert_links(self, force=False):
        ODataConverter.convert_links(self, force)

        """ create tmp file """
        import csv
        import uuid
        import os
        import copy

        tmp_file = os.path.abspath(self._dir) + '/links.csv.' + uuid.uuid4().hex.upper()[0:6]

        self._builder.build()

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

                        """
                        SAP SC: only consider:
                        1) one directional lineage
                        2) no self-referal lineage
                        3) no backwarts parent lineage
                        """
                        
                        entitytype_from = prop.association.end_roles[0].entity_type.name
                        entitytype_to = prop.association.end_roles[1].entity_type.name

                        if entitytype_from != et.name:
                            continue

                        if entitytype_from == entitytype_to:
                            continue

                        if entitytype_to in self._builder._parents:

                            if self._builder._parents[entitytype_to] != entitytype_from:
                                continue
                        else:
                            continue

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
                            propertyfromto = copy.deepcopy(self._links_head)
                            propertyfromto['association'] = self._association_directionaldataflow
                            propertyfromto['fromObjectIdentity'] = entitytypefromto['fromObjectIdentity'] + '/' + prop.name
                            propertyfromto['toObjectIdentity'] = entitytypefromto['toObjectIdentity'] + '/' + 'ParentObjectID'

                            writer.writerow(propertyfromto)

                            propertyfromto = copy.deepcopy(self._links_head)
                            propertyfromto['association'] = self._association_directionaldataflow
                            propertyfromto['fromObjectIdentity'] = entitytypefromto['fromObjectIdentity'] + '/' + 'ObjectID'
                            propertyfromto['toObjectIdentity'] = entitytypefromto['toObjectIdentity'] + '/' + 'ParentObjectID'

                            writer.writerow(propertyfromto)
       
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
