import pandas as pd
from .odata_converter import ODataConverter

import os
import logging

logger = logging.getLogger(__name__)

class CDGCAdaptor():
    def __init__(self, converter: ODataConverter):
        self._converter = converter

    def convert_csv(self):
        objects_dir = self._converter._dir + '/cdgc/objects'
        links_dir = self._converter._dir + '/cdgc/links'

        if not os.path.exists(objects_dir):
            os.makedirs(objects_dir)
        
        if not os.path.exists(links_dir):
            os.makedirs(links_dir)
        
        objects_csv = self._converter._dir + '/objects.csv'
        links_csv = self._converter._dir + '/links.csv'

        if not os.path.exists(objects_csv):
            logger.info('[... objects.csv not found ...]')
            return

        if not os.path.exists(links_csv):
            logger.info('[... links.csv not found ...]')
            return
        
        CDGCAdaptor.convert_objects_csv(objects_csv, objects_dir)
        CDGCAdaptor.convert_links_csv(links_csv, links_dir)

    @staticmethod
    def convert_objects_csv(csv, dir):
        if not os.path.exists(dir):
            logger.info(f'[... Directory is {dir} not found ...]')
            return

        logger.info('[... Start converting objects.csv ...]')

        df = pd.read_csv(csv)
        types = df['class'].unique()

        for type in types:
            df_type = df.loc[df['class'] == type]
            df_type = df_type.drop('class', axis=1)
            df_type.rename(columns={'identity':'core.externalId'}, inplace=True)

            filename = dir + '/' + type + '.csv'
            df_type.to_csv(filename, sep=',', index=False, encoding='utf-8')

            with open(filename, 'r') as file:
                filedata = file.read()
                filedata = filedata.replace('com.infa.ldm.relational', 'com.infa.odin.models.relational')
                file.close()

                with open(filename, 'w') as file:
                    file.write(filedata)
                    file.close()

            logger.info(f'[... Save converted {filename} ...]')

    @staticmethod
    def convert_links_csv(csv, dir):
        if not os.path.exists(dir):
            logger.info(f'[... Directory is {dir} not found ...]')
            return

        logger.info('[... Start converting links.csv ...]')

        df = pd.read_csv(csv)
        df = df.reindex(['fromObjectIdentity','toObjectIdentity','association'], axis=1)
        df.rename(columns={'fromObjectIdentity':'Source'}, inplace=True)
        df.rename(columns={'toObjectIdentity':'Target'}, inplace=True)
        df.rename(columns={'association':'Association'}, inplace=True)

        """
        drop redandunt core.ResourceParentChild
        """
        df = df.drop(df.loc[df['Association'] == 'core.ResourceParentChild'].index)
        df.loc[-1] = ['$resource', 'Endpoint', 'core.ResourceParentChild']
        df.index = df.index + 1
        df = df.sort_index()

        """
        drop duplicates
        """
        df.drop_duplicates(keep=False, inplace=True)

        filename = dir + '/links.csv'
        df.to_csv(filename, sep=',', index=False, encoding='utf-8')

        logger.info(f'[... Save converted {filename} ...]')

      