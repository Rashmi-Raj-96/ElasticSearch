from datetime import datetime
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from const import (
    SAMPLE_DATA_DIR,
    ES_HOST,
    ES_PORT
)

es = Elasticsearch(host=ES_HOST, port=ES_PORT)
es = Elasticsearch()


def load_jsondata():
    with open(SAMPLE_DATA_DIR) as f:
        return json.load(f)


def insert_data_by_bulk(data):
    res = helpers.bulk(es, data)  # helper is method of es
    print(res)


def insert_array_data_in_bulk(index):
    j = 0
    actions = []
    while (j <= 10):
        action = {
            "_index": index,
            "_id": j,
            "text" :str(j),
            "timestamp": datetime.now()
            }
        actions.append(action)
        j += 1
    res = helpers.bulk(es, actions)
    print(res)


if __name__ == "__main__":
    demo_data_2 = load_jsondata()
    insert_data_by_bulk(demo_data_2)
    index="test-bulk-index"
    insert_array_data_in_bulk(index)
