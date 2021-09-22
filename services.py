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

def upload_to_gcp(bucket_name, source_file_name, destination_blob_name):
    """
    Upload a file to the specified GCP Bucket with the given blob name.

    :param bucket_name: the destination GCP Bucket
    :param source_file_name: the filepath to upload
    :param destination_blob_name: the blob name to use as the destination
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name, timeout=300, num_retries=2)