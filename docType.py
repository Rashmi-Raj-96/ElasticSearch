from datetime import datetime
from elasticsearch import Elasticsearch

# manual config
# es = Elasticsearch(HOST="http://localhsot",PORT=9200)
# default config
es = Elasticsearch()

# creating index
data = {
    "author": "Chestermo",
    "text": 'Elasticsearch-insert-Demo',
    "timestamp": datetime.now(),
}


def create_index(index):
    es.indices.create(index=index, ignore=400)


def insert_one_data(_index, data):
    # index and doc_type you can customize by yourself
    res = es.index(index=_index, id=5, body=data)
    # index will return insert info: like as created is True or False
    print(res)
    """
    {'_index': 'test-index', '_type': 'authors', '_id': '5', '_version': 1, 'result': 'created', '_shards': {'total': 2, 's
uccessful': 1, 'failed': 0}, '_seq_no': 4, '_primary_term': 1}
    """

def insert_data_with_doc(index,doc_type) :
    res = es.index(index=index, id=1, doc_type=doc_type,body=data)


if __name__ == "__main__":
    index = "test-bulk-index"
    _doc_type="Demo"
    create_index(index)
    insert_data_with_doc(index,_doc_type)
    index = "test-index"
    insert_one_data(index, data)
