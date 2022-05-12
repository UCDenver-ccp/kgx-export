import http.client
import json
import logging
import os
import csv
import sqlalchemy
from typing import Iterator

from google.cloud import storage


def get_normalized_nodes(curie_list: list[str]) -> dict: # pragma: no cover
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


def get_normalized_nodes_by_parts(curie_list: list[str], sublist_size: int=1000) -> dict: # pragma: no cover
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


def upload_to_gcp(bucket_name: str, source_file_name: str, destination_blob_name: str, delete_source_file: bool=False) -> None: # pragma: no cover
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


def get_from_gcp(bucket_name: str, blob_name: str, destination_file_name: str) -> None: # pragma: no cover
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    logging.info(f'Downloading {blob_name} to {destination_file_name}')
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination_file_name)


def update_node_metadata(node: list[str], node_metadata_dict: dict, source: str) -> dict:
    """
    Updates a node metadata dictionary with information from a single node

    :param node: the node to add to the dictionary
    :param node_metadata_dict: the metadata dictionary
    :param source: the original knowledge source
    :returns the updated node metadata dictionary
    """
    category = node[2]
    prefix = node[0].split(':')[0]
    if category in node_metadata_dict:
        if prefix not in node_metadata_dict[category]["id_prefixes"]:
            node_metadata_dict[category]["id_prefixes"].append(prefix)
        node_metadata_dict[category]["count"] += 1
        node_metadata_dict[category]["count_by_source"]["original_knowledge_source"][source] += 1
    else:
        node_metadata_dict[category] = {
            "id_prefixes": [prefix],
            "count": 1,
            "count_by_source": {
                "original_knowledge_source": {
                    source: 1
                }
            }
        }
    return node_metadata_dict


def update_edge_metadata(edge: list, edge_metadata_dict: dict, node_dict: dict, source: str) -> dict:
    """
    Updates an edge metadata dictionary with information from a single edge

    :param edge: the edge to add to the dictionary
    :param edge_metadata_dict: the metadata dictionary
    :param node_dict: the normalization dictionary
    :param source: the original knowledge source
    :returns the updated edge metadata dictionary
    """
    object_category = get_category(edge[0], normalized_nodes=node_dict)
    subject_category = get_category(edge[2], normalized_nodes=node_dict)
    triple = f"{object_category}|{edge[1]}|{subject_category}"
    relation = edge[4]
    if triple in edge_metadata_dict:
        if relation not in edge_metadata_dict[triple]["relations"]:
            edge_metadata_dict[triple]["relations"].append(relation)
        edge_metadata_dict[triple]["count"] += 1
        edge_metadata_dict[triple]["count_by_source"]["original_knowledge_source"][source] += 1
    else:
        edge_metadata_dict[triple] = {
            "subject": subject_category,
            "predicate": edge[1],
            "object": object_category,
            "relations": [relation],
            "count": 1,
            "count_by_source": {
                "original_knowledge_source": {
                    source: 1
                }
            }
        }
    return edge_metadata_dict


def get_category(curie: str, normalized_nodes: dict[str, dict]) -> str:
    """
    Retrieves the category of the given curie, as determined by the normalized dictionary (with some default values)

    :param curie: the curie
    :param normalized_nodes: the normalization dictionary
    :returns the category of the curie
    """
    category = 'biolink:SmallMolecule' if curie.startswith('DRUGBANK') else 'biolink:NamedThing'
    if curie in normalized_nodes and normalized_nodes[curie] is not None and 'type' in normalized_nodes[curie]:
        category = normalized_nodes[curie]["type"][0]
    return category


def is_normal(curie: str, normalized_nodes: dict[str, dict]) -> bool:
    """
    Determines if the given curie exists in the given normalized dictionary and has the necessary fields populated

    :param curie: the curie
    :param normalized_nodes: the normalization dictionary
    :returns true if the curie exists and is useable, false otherwise
    """
    return curie in normalized_nodes and normalized_nodes[curie] is not None and \
           'id' in normalized_nodes[curie] and 'label' in normalized_nodes[curie]['id'] and \
           (not curie.startswith('CHEBI') or \
           'biolink:SmallMolecule' == normalized_nodes[curie]['type'][0] if 'type' in normalized_nodes[curie] else 'biolink:NamedThing')


def get_kgx_nodes(curies: list[str], normalized_nodes:dict[str, dict]) -> Iterator[list[str]]:
    """
    Get the KGX node representation of a curie

    :param curies: the list of curies to turn into KGX nodes
    :param normalized_nodes: a dictionary of normalized nodes, for retrieving canonical label and category
    """
    for curie in curies:
        category = 'biolink:SmallMolecule' if curie.startswith('DRUGBANK') else 'biolink:NamedThing'
        if is_normal(curie, normalized_nodes):
            name = normalized_nodes[curie]['id']['label']
            if 'type' in normalized_nodes[curie]:
                category = normalized_nodes[curie]['type'][0]
            yield [curie, name, category]
        else:
            yield []
