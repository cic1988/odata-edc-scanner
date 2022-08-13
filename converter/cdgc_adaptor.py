import pandas as pd
from .odata_converter import ODataConverter

import logging

logger = logging.getLogger(__name__)

class CDGCAdaptor():

    def __init__(self, converter: ODataConverter):
        self._converter = converter

        import os
        object_csv_path = os.path.abspath(self._converter._dir) + '/objects.csv'
        
        if not os.path.exists(object_csv_path):
            logger.info('[... objects.csv not found ...]')
            return

        self._objects_frame = pd.read_csv(object_csv_path)

        link_csv_path = os.path.abspath(self._converter._dir) + '/links.csv'
        
        if not os.path.exists(link_csv_path):
            logger.info('[... links.csv not found ...]')
            return
        
        self._links_frame = pd.read_csv(link_csv_path)
    

    def convert_objects_csv(self):
        if not hasattr(self, '_objects_frame'):
            return
    
    @staticmethod
    def convert_objects_csv(csv):
        df = pd.read_csv(csv)
        print(df)

        return
        
        



        