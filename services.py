import http.client
import json
import logging
import os

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
    logging.debug(f'Splitting the {len(curie_list)} length list of curies by {sublist_size}')
    for cap in range(start, end, sublist_size):
        curies = curie_list[cap - sublist_size : cap]
        node_subset = get_normalized_nodes(curies)
        nodes.update(node_subset)
        logging.debug(f'up to {len(nodes.keys())} nodes')
    curies = curie_list[-extra:]
    node_subset = get_normalized_nodes(curies)
    nodes.update(node_subset)
    logging.info(f'Final total: {len(nodes.keys())} nodes')
    return nodes

def upload_to_gcp(bucket_name, source_file_name, destination_blob_name, delete_source_file=False):
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
    if blob.exists() and os.isfile(source_file_name) and delete_source_file:
        os.remove(source_file_name)


def compose_gcp_files(bucket_name, directory, file_prefix, new_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    new_file_blob = bucket.blob(f"{directory}{new_file_name}")
    if new_file_blob.exists():
        logging.warn(f"Deleting existing file {directory}{new_file_name}")
        new_file_blob.delete()
    logging.info(f"Composing '{file_prefix}' files in '{directory}' into {new_file_name}")
    matching_files = client.list_blobs(bucket, prefix=f"{directory}{file_prefix}")
    new_file_blob.compose([blob for blob in matching_files])


def remove_temp_files(bucket_name, file_list):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for filename in file_list:
        temp_blob = bucket.blob(filename)
        if temp_blob.exists():
            logging.info(f"Deleting {filename}")
            temp_blob.delete()
        else:
            logging.warn(f"Could not find file for deletion: {filename}")