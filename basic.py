from datetime import datetime
from elasticsearch import Elasticsearch

# manual config
# es = Elasticsearch(HOST="http://localhsot",PORT=9200)
# default config
es = Elasticsearch()


# creating index with default type doc
def create_body():
    doc = {
        'author': 'Raj',
        'text': 'Elasticsearch-Demo',
        'timestamp': datetime.now(),
    }
    return doc


def create_index(_index, _id, def_body):
    # for the first time result will be created and then it will be updated
    res = es.index(index=_index, id=_id, body=def_body)
    # print(res['result'])
    print(res)


def get_index_by_id(_index, _id):
    res = es.get(index=_index, id=_id)
    print(res['_source'])


if __name__ == "__main__":
    body = create_body()
    index = "test-index"
    create_index(index, 1, body)
    get_index_by_id(index, 1)
