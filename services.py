import http.client
import json
import logging
import os
import gzip
import math
import shutil
from typing import Iterator

from google.cloud import storage

PRIMARY_KNOWLEDGE_SOURCE = "infores:text-mining-provider-targeted"


def get_normalized_nodes(curie_list: list[str]) -> dict:  # pragma: no cover
    """
    Use the SRI Node Normalization service to get detailed node information from curies

    :param curie_list: the list of curies to normalize
    """
    json_data = json.dumps({'curies': curie_list, 'conflate': False})
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    conn = http.client.HTTPSConnection(host='nodenormalization-sri.renci.org')
    try:
        conn.request('POST', '/get_normalized_nodes', body=json_data, headers=headers)
        response = conn.getresponse()
        if response.status == 200:
            return json.loads(response.read())
    finally:
        conn.close()
    logging.warning("Failed to get normalized nodes")
    return {}


def get_normalized_nodes_by_parts(curie_list: list[str], sublist_size: int = 1000) -> dict:  # pragma: no cover
    """
    Use the SRI Node Normalization service to get detailed node information from curies, with a maxiumum number of curies per HTTP call

    :param curie_list: the list of curies to normalize
    :param sublist_size: the maximum number of curies per HTTP call
    """
    nodes = {}
    start = sublist_size
    end = len(curie_list)
    extra = end % sublist_size
    logging.debug(f'Splitting the {len(curie_list)} length list of curies by {sublist_size}')
    for cap in range(start, end, sublist_size):
        curies = curie_list[cap - sublist_size: cap]
        node_subset = get_normalized_nodes(curies)
        nodes.update(node_subset)
        logging.debug(f'up to {len(nodes.keys())} nodes')
    curies = curie_list[-extra:]
    node_subset = get_normalized_nodes(curies)
    nodes.update(node_subset)
    logging.info(f'Final total: {len(nodes.keys())} nodes')
    return nodes


def upload_to_gcp(bucket_name: str, source_file_name: str, destination_blob_name: str, delete_source_file: bool = False) -> None:  # pragma: no cover
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


def get_from_gcp(bucket_name: str, blob_name: str, destination_file_name: str) -> None:  # pragma: no cover
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
    :param source: the primary knowledge source
    :returns the updated node metadata dictionary
    """
    category = node[2]
    prefix = node[0].split(':')[0]
    if category in node_metadata_dict:
        if prefix not in node_metadata_dict[category]["id_prefixes"]:
            node_metadata_dict[category]["id_prefixes"].append(prefix)
        node_metadata_dict[category]["count"] += 1
        node_metadata_dict[category]["count_by_source"]["primary_knowledge_source"][source] += 1
    else:
        node_metadata_dict[category] = {
            "id_prefixes": [prefix],
            "count": 1,
            "count_by_source": {
                "primary_knowledge_source": {
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
    :param source: the primary knowledge source
    :returns the updated edge metadata dictionary
    """
    object_category = get_category(edge[0], normalized_nodes=node_dict)
    subject_category = get_category(edge[2], normalized_nodes=node_dict)
    triple = f"{object_category}|{edge[1]}|{subject_category}"
    relation = edge[14]
    if triple in edge_metadata_dict:
        if relation not in edge_metadata_dict[triple]["relations"]:
            edge_metadata_dict[triple]["relations"].append(relation)
        edge_metadata_dict[triple]["count"] += 1
        edge_metadata_dict[triple]["count_by_source"]["primary_knowledge_source"][source] += 1
    else:
        edge_metadata_dict[triple] = {
            "subject": subject_category,
            "predicate": edge[1],
            "object": object_category,
            "relations": [relation],
            "count": 1,
            "count_by_source": {
                "primary_knowledge_source": {
                    source: 1
                }
            }
        }
    return edge_metadata_dict


def update_edge_metadata_2(edge, edge_metadata_dict: dict, node_dict: dict, source: str) -> dict:
    """
    Updates an edge metadata dictionary with information from a single edge

    :param edge: the edge to add to the dictionary
    :param edge_metadata_dict: the metadata dictionary
    :param node_dict: the normalization dictionary
    :param source: the primary knowledge source
    :returns the updated edge metadata dictionary
    """
    sub = edge['subject_uniprot'] if edge['subject_uniprot'] else edge['subject_curie']
    obj = edge['object_uniprot'] if edge['object_uniprot'] else edge['object_curie']
    object_category = get_category(obj, normalized_nodes=node_dict)
    subject_category = get_category(sub, normalized_nodes=node_dict)
    triple = f"{object_category}|{edge['predicate_curie']}|{subject_category}"
    relation = edge['association_curie']
    if triple in edge_metadata_dict:
        if relation not in edge_metadata_dict[triple]["relations"]:
            edge_metadata_dict[triple]["relations"].append(relation)
        edge_metadata_dict[triple]["count"] += 1
        edge_metadata_dict[triple]["count_by_source"]["primary_knowledge_source"][source] += 1
    else:
        edge_metadata_dict[triple] = {
            "subject": subject_category,
            "predicate": edge['predicate_curie'],
            "object": object_category,
            "relations": [relation],
            "count": 1,
            "count_by_source": {
                "primary_knowledge_source": {
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
    type_check = True
    if curie.startswith('CHEBI') and normalized_nodes[curie] is not None:
        if not 'type' in normalized_nodes[curie]:
            type_check = False
        elif normalized_nodes[curie]['type'][0] not in \
            ['biolink:SmallMolecule', 'biolink:MolecularMixture', 'biolink:Drug']:
            type_check = False
    return curie in normalized_nodes \
        and normalized_nodes[curie] is not None \
        and 'id' in normalized_nodes[curie] \
        and 'label' in normalized_nodes[curie]['id'] \
        and type_check


def get_kgx_nodes(curies: list[str], normalized_nodes: dict[str, dict]) -> Iterator[list[str]]:
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


def get_aggregate_score(rows):
    scores = []
    for row in rows:
        scores.append(get_score(row))
    return math.fsum(scores) / float(len(scores))


def get_score(row):
    base_score = float(row['score'])
    if not row['subject_idf'] or not row['object_idf']:
        return base_score
    else:
        return abs(math.log10(row['subject_idf']) * math.log10(row['object_idf']) * base_score)


def get_assertion_json(rows):
    semmed_count = sum([row['semmed_flag'] for row in rows])
    row1 = rows[0]
    supporting_publications = []
    for row in rows:
        document_id = row['document_id']
        if document_id.startswith('PMC') and ':' not in document_id:
            supporting_publications.append(document_id.replace('PMC', 'PMC:'))
        else:
            supporting_publications.append(document_id)
    attributes_list = [
        {
            "attribute_type_id": "biolink:primary_knowledge_source",
            "value": "infores:text-mining-provider-targeted",
            "value_type_id": "biolink:InformationResource",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:supporting_data_source",
            "value": "infores:pubmed",
            "value_type_id": "biolink:InformationResource",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:evidence_count",
            "value": row1['evidence_count'],
            "value_type_id": "biolink:EvidenceCount",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:extraction_confidence_score",
            "value": get_aggregate_score(rows),
            "value_type_id": "biolink:ConfidenceLevel",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:publications",
            "value": supporting_publications,
            "value_type_id": "biolink:Uriorcurie",
            "attribute_source": "infores:pubmed"
        }
    ]
    if semmed_count > 0:
        attributes_list.append({
            "attribute_type_id": "biolink:semmed_agreement_count",
            "value": semmed_count,
            "value_type_id": "SIO:000794",
            "attribute_source": "infores:text-mining-provider-targeted"
        })
    for row in rows:
        attributes_list.append(get_evidence_json(row))
    return json.dumps(attributes_list)


def get_evidence_json(row):
    document_id = row['document_id']
    if document_id.startswith('PMC') and ':' not in document_id:
        document_id = document_id.replace('PMC', 'PMC:')
    nested_attributes = [
        {
            "attribute_type_id": "biolink:supporting_text",
            "value": row['sentence'],
            "value_type_id": "EDAM:data_3671",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:publications",
            "value": document_id,
            "value_type_id": "biolink:Uriorcurie",
            "value_url": f"https://pubmed.ncbi.nlm.nih.gov/{str(row['document_id']).split(':')[-1]}/",
            "attribute_source": "infores:pubmed"
        },
        {
            "attribute_type_id": "biolink:supporting_text_located_in",
            "value": row['document_zone'],
            "value_type_id": "IAO_0000314",
            "attribute_source": "infores:pubmed"
        },
        {
            "attribute_type_id": "biolink:extraction_confidence_score",
            "value": get_score(row),
            "value_type_id": "EDAM:data_1772",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:subject_location_in_text",
            "value": row['subject_span'] if row['subject_span'] else '',
            "value_type_id": "SIO:001056",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:object_location_in_text",
            "value": row['object_span'] if row['object_span'] else '',
            "value_type_id": "SIO:001056",
            "attribute_source": "infores:text-mining-provider-targeted "
        }
    ]
    if row['document_year']:
        nested_attributes.append(
            {
                "attribute_type_id": "biolink:supporting_document_year",
                "value": row['document_year'],
                "value_type_id": "UO:0000036",
                "attribute_source": "infores:pubmed"
            }
        )
    if row['semmed_flag'] == 1:
        nested_attributes.append(
            {
                "attribute_type_id": "biolink:agrees_with_data_source",
                "value": "infores:semmeddb",
                "value_type_id": "biolink:InformationResource",
                "attribute_source": "infores:text-mining-provider-targeted"
            }
        )
    return {
        "attribute_type_id": "biolink:has_supporting_study_result",
        "value": f"tmkp:{row['evidence_id']}",
        "value_type_id": "biolink:TextMiningResult",
        "value_url": f"https://tmui.text-mining-kp.org/evidence/{row['evidence_id']}",
        "attribute_source": "infores:text-mining-provider-targeted",
        "attributes": nested_attributes
    }


def get_edge(rows, predicate):
    relevant_rows = [row for row in rows if row['predicate_curie'] == predicate]
    if len(relevant_rows) == 0:
        logging.debug(f'No relevant rows for predicate {predicate}')
        return None
    row1 = relevant_rows[0]
    if (row1['object_curie'].startswith('PR:') and not row1['object_uniprot']) or \
            (row1['subject_curie'].startswith('PR:') and not row1['subject_uniprot']):
        logging.debug(f"Could not get uniprot for pr curie ({row1['object_curie']}|{row1['subject_curie']})")
        return None
    sub = row1['subject_uniprot'] if row1['subject_uniprot'] else row1['subject_curie']
    obj = row1['object_uniprot'] if row1['object_uniprot'] else row1['object_curie']
    supporting_study_results = '|'.join([f"tmkp:{row['evidence_id']}" for row in relevant_rows])
    supporting_publications = []
    for row in relevant_rows:
        document_id = row['document_id']
        if document_id.startswith('PMC') and ':' not in document_id:
            supporting_publications.append(document_id.replace('PMC', 'PMC:'))
        else:
            supporting_publications.append(document_id)
    supporting_publications_string = '|'.join(supporting_publications)
    qualified_predicate = ''
    subject_aspect_qualifier = ''
    subject_direction_qualifier = ''
    subject_part_qualifier = ''
    subject_form_or_variant_qualifier = ''
    object_aspect_qualifier = ''
    object_direction_qualifier = ''
    object_part_qualifier = ''
    object_form_or_variant_qualifier = ''
    anatomical_context_qualifier = ''
    if predicate == 'biolink:entity_positively_regulates_entity':
        predicate = 'biolink:affects'
        qualified_predicate = 'biolink:causes'
        object_aspect_qualifier = 'activity_or_abundance'
        object_direction_qualifier = 'increased'
    elif predicate == 'biolink:entity_negatively_regulates_entity':
        predicate = 'biolink:affects'
        qualified_predicate = 'biolink:causes'
        object_aspect_qualifier = 'activity_or_abundance'
        object_direction_qualifier = 'decreased'
    elif predicate == 'biolink:gain_of_function_contributes_to':
        predicate = 'biolink:affects'
        qualified_predicate = 'biolink:contributes_to'
        subject_form_or_variant_qualifier = 'gain_of_function_variant_form'
    elif predicate == 'biolink:loss_of_function_contributes_to':
        predicate = 'biolink:affects'
        qualified_predicate = 'biolink:contributes_to'
        subject_form_or_variant_qualifier = 'loss_of_function_variant_form'
    return [sub, predicate, obj, qualified_predicate,
            subject_aspect_qualifier, subject_direction_qualifier,
            subject_part_qualifier, subject_form_or_variant_qualifier,
            object_aspect_qualifier, object_direction_qualifier,
            object_part_qualifier, object_form_or_variant_qualifier,
            anatomical_context_qualifier,
            row1['assertion_id'], row1['association_curie'], get_aggregate_score(relevant_rows),
            supporting_study_results, supporting_publications_string, get_assertion_json(relevant_rows)]


def write_edges(edge_dict, nodes, output_filename):
    logging.info("Starting edge output")
    skipped_assertions = set([])
    with open(output_filename, 'a') as outfile:
        for assertion, rows in edge_dict.items():
            row1 = rows[0]
            sub = row1['subject_uniprot'] if row1['subject_uniprot'] else row1['subject_curie']
            obj = row1['object_uniprot'] if row1['object_uniprot'] else row1['object_curie']
            if sub not in nodes or obj not in nodes:
                continue
            predicates = set([row['predicate_curie'] for row in rows])
            for predicate in predicates:
                edge = get_edge(rows, predicate)
                if not edge:
                    skipped_assertions.add(assertion)
                    continue
                line = '\t'.join(str(val) for val in edge) + '\n'
                throwaway_value = outfile.write(line)
        outfile.flush()
    logging.info(f'{len(skipped_assertions)} distinct assertions were skipped')
    logging.info("Edge output complete")


def compress(infile, outfile):
    with open(infile, 'rb') as textfile:
        with gzip.open(outfile, 'wb') as gzfile:
            shutil.copyfileobj(textfile, gzfile)

def decompress(infile, outfile):
    with gzip.open(infile, 'rb') as gzfile:
        with open(outfile, 'wb') as textfile:
            shutil.copyfileobj(gzfile, textfile)


def generate_metadata(edgefile, nodefile, outdir):
    node_headers = ['id', 'name', 'category']
    edge_headers = ['subject', 'predicate', 'object', 'qualified_predicate',
               'subject_aspect_qualifier', 'subject_direction_qualifier',
               'subject_part_qualifier', 'subject_form_or_variant_qualifier',
               'object_aspect_qualifier', 'object_direction_qualifier',
               'object_part_qualifier', 'object_form_or_variant_qualifier',
               'anatomical_context_qualifier', 'id', 'relation', 'confidence_score',
               'supporting_study_results', 'supporting_publications', '_attributes']
    if not os.path.isdir(outdir):
        os.mkdir(outdir)
    node_metadata_dict = {}
    node_file = os.path.join(outdir, "nodes.tsv")
    edge_file = os.path.join(outdir, "edges.tsv")
    metadata_file = os.path.join(outdir, "content_metadata.json")

    node_lines = gzip.open(nodefile, 'rb').readlines()
    nodes = [node_line.decode().strip().split('\t') for node_line in node_lines]
    curies = [node[0] for node in nodes]
    for node in nodes:
        node_metadata_dict = update_node_metadata(node, node_metadata_dict, PRIMARY_KNOWLEDGE_SOURCE)
    normalized_nodes = get_normalized_nodes(curies)

    edge_metadata_dict = {}
    with gzip.open(edgefile, 'rb') as infile:
        for line in infile:
            cols = line.decode().split('\t')
            edge_metadata_dict = update_edge_metadata(cols, edge_metadata_dict, normalized_nodes, PRIMARY_KNOWLEDGE_SOURCE)
    metadata_dict = {
        "nodes": node_metadata_dict,
        "edges": list(edge_metadata_dict.values())
    }
    logging.info("Writing metadata file")
    with open(metadata_file, 'w') as outfile:
        outfile.write(json.dumps(metadata_dict))
    # logging.info("Creating tarball")
    # shutil.make_archive('targeted_assertions', 'gztar', root_dir=outdir)
