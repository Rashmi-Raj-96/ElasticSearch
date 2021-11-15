from elasticsearch import Elasticsearch
from const import (
    ES_HOST,
    ES_PORT
)

es = Elasticsearch(host=ES_HOST, port=ES_PORT)
es = Elasticsearch()


def search_by_index_and_id(_index, _id):
    res = es.get(
        index=_index,
        id=_id
    )
    return res


def search_by_index_and_query(_index, _doc_type, query):
    res = es.search(
        index=_index,
        body=query
    )
    return res


if __name__ == "__main__":

    index = "test-index"
    _id = 5
    res = search_by_index_and_id(index, _id)
    print(res)
    _doc_type = "Demo"
    index = "test-bulk-index"
    query = {"query": {"match_all": {}}}
    res = search_by_index_and_query(index, _doc_type, query)
    print(res)
