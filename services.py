import http.client
import json
import logging
from google.cloud import storage


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


def get_normalized_nodes_by_parts(curie_list, sublist_size=1000) -> dict:
    nodes = {}
    curies = []
    start = sublist_size
    end = len(curie_list)
    extra = end % sublist_size
    logging.info(f'Splitting the {len(curie_list)} length list of curies by {sublist_size}')
    for cap in range(start, end, sublist_size):
        curies = curie_list[cap - sublist_size : cap]
        node_subset = get_normalized_nodes(curies)
        nodes.update(node_subset)
        logging.info(f'up to {len(nodes.keys())} nodes')
    curies = curie_list[-extra:]
    node_subset = get_normalized_nodes(curies)
    nodes.update(node_subset)
    logging.info(f'Final total: {len(nodes.keys())} nodes')
    return nodes

def upload_to_gcp(bucket_name, source_file_name, destination_blob_name):
    """
    Upload a file to the specified GCP Bucket with the given blob name.

    :param bucket_name: the destination GCP Bucket
    :param source_file_name: the filepath to upload
    :param destination_blob_name: the blob name to use as the destination
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    logging.info(f'Uploading {source_file_name} to {destination_blob_name}')
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name, timeout=300, num_retries=2)