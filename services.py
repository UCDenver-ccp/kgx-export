import datetime
import http.client
import json


def get_normalized_nodes(curie_list):
    log_timestamp("Start GNN")
    json_data = json.dumps({'curies': curie_list})
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    conn = http.client.HTTPSConnection(host='nodenormalization-sri-dev.renci.org')
    conn.request('POST', '/1.1/get_normalized_nodes', body=json_data, headers=headers)
    response = conn.getresponse()
    if response.status == 200:
        log_timestamp("Returning GNN")
        return json.loads(response.read())
    log_timestamp("Failed GNN")
    return {}


def log_timestamp(pretext):
    print(f"{pretext}: {datetime.datetime.now()}")
