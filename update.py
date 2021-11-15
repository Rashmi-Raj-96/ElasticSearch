from elasticsearch import Elasticsearch
from elasticsearch import helpers
from const import (
    ES_HOST,
    ES_PORT
)

es = Elasticsearch(host=ES_HOST, port=ES_PORT)
es = Elasticsearch()


# update by index
def update_data_by_index(_index, _id, update_data):
    res = es.update(
        index=_index,
        id=_id,
        body=update_data
    )
    return res


# update by query
def update_by_query(_index, query, field, update_data):
    _inline = "ctx._source.{field}={update_data}".format(field=field, update_data=update_data)
    _query = {
        "script": {
            "inline": _inline,
        },
        "query": {
            "match": query
        }
    }
    res = es.update_by_query(index=_index, body=_query)
    return res


# update by bulk api
def update_by_bulk(_index, _id, update_data):
    action = [{
        "_id": _id,
        "_index": _index,
        "_op_type": 'update',
        "doc": {"text": update_data}
    }]
    res = helpers.bulk(es, action)
    return res


if __name__ == "__main__":
    index = "test-index"
    _id = 5
    update_data = {
        "doc": {"text": "Update-data"}
    }
    res = update_data_by_index(index, _id, update_data)
    print(res)
    field = "text"
    update_data = "update-by-index"
    query = {"id" : 5}
    res = update_by_query(index, query,field, update_data)
    print(res)
    index = "test-bulk-index"
    update_data = "updated data"
    _id = 1
    res = update_by_bulk(index, _id, update_data)
    print(res)
