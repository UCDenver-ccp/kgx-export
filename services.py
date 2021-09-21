import http.client
import json
import logging


def get_normalized_nodes(curie_list):
    json_data = json.dumps({'curies': curie_list, 'conflate': False})
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    conn = http.client.HTTPSConnection(host='nodenormalization-sri.renci.org')
    conn.request('POST', '/1.2/get_normalized_nodes', body=json_data, headers=headers)
    response = conn.getresponse()
    if response.status == 200:
        return json.loads(response.read())
    logging.warning("Failed to get normalized nodes")
    return {}
