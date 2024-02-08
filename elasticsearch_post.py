from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

from db_manager import SQLManager

MEDBREVIA_NEW_INDEX_BACKEND_KEY = ""

class ElasticPush:
    def __init__(self,index_name="search-medbrevia-pubmed-articles",timeout=3600):
        self.index_name = index_name
        self.es_ip = ""

        self.refresh_connection()
        

    def refresh_connection(self):
        self.es = Elasticsearch(cloud_id="", 
                                     api_key=MEDBREVIA_NEW_INDEX_BACKEND_KEY)
        

    def get_fixed_pub_date(pub_date):
        if pub_date:
            try:
                # Parse the date using strptime and then reformat using strftime
                # This will ensure the date is in 'YYYY-MM-DD' format
                parsed_date = datetime.strptime(pub_date, '%Y-%m-%d')
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                pass
        return None
     
    def generate_dict(self, data: dict) -> dict:
        d = data.copy()
        d['_op_type'] = 'index'
        d['_index'] = self.index_name
        d['_id'] = d['pubmed_id']
        d['@timestamp'] = datetime.utcnow().isoformat()  # Ensure the timestamp is in ISO format
        d['@last_update'] = datetime.utcnow().isoformat()
        d['pub_date'] = ElasticPush.get_fixed_pub_date(data.get('pub_date'))
        return d

    def bulk_insert(self, data_list):
        self.refresh_connection()

        # Function to perform bulk insert
        def insert_chunk(chunk):
            documents = [self.generate_dict(data) for data in chunk if data]
            num_success, num_failed = helpers.bulk(self.es, documents, stats_only=True)
            return num_success, num_failed

        # Split data list into chunks of 15,000 documents each
        chunk_size = 15000
        chunks = [data_list[i:i + chunk_size] for i in range(0, len(data_list), chunk_size)]

        total_success, total_failed = 0, 0
        for chunk in chunks:
            num_success, num_failed = insert_chunk(chunk)
            total_success += num_success
            total_failed += num_failed

        return total_failed == 0


    def refresh(self):
        self.refresh_connection()
        return self.es.indices.refresh(index=self.index_name)