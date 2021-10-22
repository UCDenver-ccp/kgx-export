import http.client
import json
import logging
import os
import csv
import sqlalchemy

from google.cloud import storage


def get_normalized_nodes(curie_list: list[str]) -> dict:
    """
    Use the SRI Node Normalization service to get detailed node information from curies

    :param curie_list: the list of curies to normalize
    """
    json_data = json.dumps({'curies': curie_list, 'conflate': False})
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    conn = http.client.HTTPSConnection(host='nodenormalization-sri.renci.org')
    try:
        conn.request('POST', '/1.2/get_normalized_nodes', body=json_data, headers=headers)
        response = conn.getresponse()
        if response.status == 200:
            return json.loads(response.read())
    finally:
        conn.close()
    logging.warning("Failed to get normalized nodes")
    return {}


def get_normalized_nodes_by_parts(curie_list: list[str], sublist_size: int=1000) -> dict:
    """
    Use the SRI Node Normalization service to get detailed node information from curies, with a maxiumum number of curies per HTTP call

    :param curie_list: the list of curies to normalize
    :param sublist_size: the maximum number of curies per HTTP call
    """
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


def upload_to_gcp(bucket_name: str, source_file_name: str, destination_blob_name: str, delete_source_file: bool=False) -> None:
    """
    Upload a file to the specified GCP Bucket with the given blob name.

    :param bucket_name: the destination GCP Bucket
    :param source_file_name: the filepath to upload
    :param destination_blob_name: the blob name to use as the destination
    :param delete_source_file: whether or not to delete the local file after upload
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    logging.info(f'Uploading {source_file_name} to {destination_blob_name}')
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name, timeout=300, num_retries=2)
    if blob.exists() and os.path.isfile(source_file_name) and delete_source_file:
        os.remove(source_file_name)


def get_from_gcp(bucket_name: str, blob_name: str, destination_file_name: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    logging.info(f'Downloading {blob_name} to {destination_file_name}')
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination_file_name)


def compose_gcp_files(bucket_name: str, directory: str, file_prefix: str, new_file_name: str) -> None:
    """
    Merge files in a Google Storage bucket

    :param bucket_name: the bucket containing the files to be merged
    :param directory: the directory prefix within the bucket
    :param file_prefix: the common prefix of the files to be merged
    :param new_file_name: the name of the new file to be created/replaced
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    new_file_blob = bucket.blob(f"{directory}{new_file_name}")
    if new_file_blob.exists():
        logging.info(f"Deleting existing file {directory}{new_file_name}")
        new_file_blob.delete()
    logging.info(f"Composing '{file_prefix}' files in '{directory}' into {new_file_name}")
    matching_files = client.list_blobs(bucket, prefix=f"{directory}{file_prefix}")
    new_file_blob.compose([blob for blob in matching_files])


def remove_temp_files(bucket_name: str, file_list: list[str]) -> None:
    """
    Delete a list of files from a Google Storage bucket

    :param bucket_name: the name of the bucket
    :param file_list: the list of filenames to delete
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for filename in file_list:
        temp_blob = bucket.blob(filename)
        if temp_blob.exists():
            logging.info(f"Deleting {filename}")
            temp_blob.delete()
        else:
            logging.warn(f"Could not find file for deletion: {filename}")


def insert_dictionary_records(session):
    from models import PRtoUniProt
    with open('in/pr-to-uniprot.tsv', 'r') as infile:
        csv_reader = csv.reader(infile, 'excel-tab')
        buffer = []
        for row in csv_reader:
            buffer.append({
                'pr': row[0],
                'uniprot': row[1],
                'taxon': row[2]
            })
            if len(buffer) % 10000 == 0:
                session.bulk_insert_mappings(PRtoUniProt, buffer)
                buffer = []
        session.bulk_insert_mappings(PRtoUniProt, buffer)
    session.commit()
