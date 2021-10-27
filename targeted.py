import gzip
import logging
import math
import os
import json
import shutil
from typing import Iterator

import models
import services
import sqlalchemy

ROW_BATCH_SIZE = 50000
HUMAN_TAXON = 'NCBITaxon:9606'


def update_node_metadata(node: list[str], node_metadata_dict: dict) -> dict:
    category = node[2]
    prefix = node[0].split(':')[0]
    if category in node_metadata_dict:
        if prefix not in node_metadata_dict[category]["id_prefixes"]:
            node_metadata_dict[category]["id_prefixes"].append(prefix)
        node_metadata_dict[category]["count"] += 1
        node_metadata_dict[category]["count_by_source"]["original_knowledge_source"]["infores:text-mining-provider-targeted"] += 1
    else:
        node_metadata_dict[category] = {
            "id_prefixes": [prefix],
            "count": 1,
            "count_by_source": {
                "original_knowledge_source": {
                    "infores:text-mining-provider-targeted": 1
                }
            }
        }
    return node_metadata_dict


def update_edge_metadata(edge: list, edge_metadata_dict: dict, node_dict: dict) -> dict:
    object_category = get_category(edge[0], normalized_nodes=node_dict)
    subject_category = get_category(edge[2], normalized_nodes=node_dict)
    triple = f"{object_category}|{edge[1]}|{subject_category}"
    relation = edge[4]
    if triple in edge_metadata_dict:
        if relation not in edge_metadata_dict[triple]["relations"]:
            edge_metadata_dict[triple]["relations"].append(relation)
        edge_metadata_dict[triple]["count"] += 1
        edge_metadata_dict[triple]["count_by_source"]["original_knowledge_source"]["infores:text-mining-provider-targeted"] += 1
    else:
        edge_metadata_dict[triple] = {
            "subject": subject_category,
            "predicate": edge[1],
            "object": object_category,
            "relations": [relation],
            "count": 1,
            "count_by_source": {
                "original_knowledge_source": {
                    "infores:text-mining-provider-targeted": 1
                }
            }
        }
    return edge_metadata_dict


def get_category(curie: str, normalized_nodes: dict[str, dict]) -> str:
    category = 'biolink:SmallMolecule' if curie.startswith('DRUGBANK') else 'biolink:NamedThing'
    if curie in normalized_nodes and normalized_nodes[curie] is not None and 'type' in normalized_nodes[curie]:
        category = normalized_nodes[curie]["type"][0]
    return category


def is_normal(curie: str, normalized_nodes: dict[str, dict]) -> bool:
    return curie in normalized_nodes and normalized_nodes[curie] is not None and \
           'id' in normalized_nodes[curie] and 'label' in normalized_nodes[curie]['id']


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


def get_node_data(session: sqlalchemy.orm.Session, use_uniprot: bool=False) -> (list[str], dict[str, dict]):
    """
    Get the subject and object curies from assertions, uniquifies the list,
    and calls the SRI Node Normalizer service to get the dictionary.

    :param session: the database session.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :returns a tuple containing the list of unique curies and the normalization dictionary.
    """
    logging.info("Getting node data")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    if use_uniprot:
        curies = [row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, subject_curie) as curie FROM assertion LEFT JOIN pr_to_uniprot ON subject_curie = pr AND taxon = "NCBITaxon:9606"')).all()]
        curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, object_curie) as curie FROM assertion LEFT JOIN pr_to_uniprot ON object_curie = pr AND taxon = "NCBITaxon:9606"')).all()])
    else:
        curies = [row[0] for row in session.query(sqlalchemy.text('DISTINCT subject_curie FROM assertion')).all()]
        curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT object_curie FROM assertion')).all()])
    curies = list(set(curies))
    logging.info(f'node curies retrieved and uniquified ({len(curies)})')
    if use_uniprot:
        curies = [curie for curie in curies if not curie.startswith('PR:')]
    normalized_nodes = services.get_normalized_nodes(curies)
    return (curies, normalized_nodes)


def write_nodes(curies: list[str], normalize_dict: dict[str, dict], output_filename: str) -> dict:
    """
    Output the node data to a gzipped TSV file according to KGX node format.

    :param curies: the list of node curies.
    :param normalize_dict: the dictionary containing normalization information for the node curies.
    :param output_filename: filepath for the output file.
    :returns a metadata dictionary for the nodes that were written to file.
    """
    logging.info("Starting node output")
    metadata_dict = {}
    with gzip.open(output_filename, 'wb') as outfile:
        for node in get_kgx_nodes(curies, normalize_dict):
            if len(node) == 0:
                continue
            line = '\t'.join(node) + '\n'
            outfile.write(line.encode('utf-8'))
            metadata_dict = update_node_metadata(node, metadata_dict)
    logging.info('Node output complete')
    return metadata_dict


def write_edges(session: sqlalchemy.orm.Session, normalize_dict: dict[str, dict], output_filename: str, use_uniprot: bool=False, limit: int=0) -> None:
    """
    Get the edge (or edges) associated with each assertion and output them to a gzipped TSV file according to KGX edge format.

    :param session: the database session.
    :param normalize_dict: the dictionary containing normalization information for checking the nodes associated with each edge.
    :param output_filename: filepath for the output file.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :param limit: the maximum number of supporting study results per edge to include in the JSON blob (0 is no limit)
    :returns a metadata dictionary for the edges that were written to file.
    """
    logging.info("Starting edge output")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    evaluation_subquery = session.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    assertion_count = session.query(models.Assertion).filter(models.Assertion.assertion_id.notin_(evaluation_subquery)).count()
    partition_count = math.ceil(assertion_count / ROW_BATCH_SIZE)
    logging.info(f"Total Assertions: {assertion_count}")
    logging.info(f"Total Partition Count: {partition_count}")
    assertion_query = sqlalchemy.select(models.Assertion)\
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))\
        .execution_options(stream_results=True)
    metadata_dict = {}
    with gzip.open(output_filename, 'wb') as outfile:
        for partition_number in range(0, partition_count):
            for assertion, in session.execute(assertion_query.offset(partition_number * ROW_BATCH_SIZE).limit(ROW_BATCH_SIZE)):
                if use_uniprot:
                    edges = assertion.get_other_edges_kgx(limit)
                else:
                    edges = assertion.get_edges_kgx(limit)
                for edge in edges:
                    if len(edge) == 0:
                        continue
                    if not (is_normal(edge[0], normalize_dict) and is_normal(edge[2], normalize_dict)):
                        continue
                    line = '\t'.join(str(val) for val in edge) + '\n'
                    outfile.write(line.encode('utf-8'))
                    metadata_dict = update_edge_metadata(edge, metadata_dict, normalize_dict)
            outfile.flush()
            logging.info(f"Done with partition {partition_number}")
    logging.info("Edge output complete")
    return metadata_dict


def create_kge_tarball(dir: str, node_metadata: dict, edge_metadata: dict):
    logging.info("Starting KGE tarball creation")
    if not os.path.isdir(dir):
        os.mkdir(dir)
    node_file = os.path.join(dir, "nodes.tsv")
    edge_file = os.path.join(dir, "edges.tsv")
    metadata_file = os.path.join(dir, "content_metadata.json")

    metadata_dict = {
        "nodes": node_metadata,
        "edges": list(edge_metadata.values())
    }
    logging.info("Writing metadata file")
    with open(metadata_file, 'w') as outfile:
        outfile.write(json.dumps(metadata_dict))

    # We extract the files from gzip if they are not already in the temp directory
    if not (os.path.isfile(node_file) and os.path.isfile(edge_file)):
        logging.info("Could not find one or more of the text files. Attempting to extract them")
        with gzip.open('nodes.tsv.gz', 'rb') as file_in:
            with open(node_file, 'wb') as file_out:
                # Currenlty the gz files don't have headers, but metadata generation requires them.
                file_out.write('id\tname\tcategory\n'.encode('utf-8'))
                shutil.copyfileobj(file_in, file_out)
        with gzip.open('edges.tsv.gz', 'rb') as file_in:
            with open(edge_file, 'wb') as file_out:
                # Currenlty the gz files don't have headers, but metadata generation requires them.
                file_out.write('subject\tpredicate\tobject\tid\trelation\tconfidence_score\tsupporting_study_results\tsupporting_publications\t_attributes\n'.encode('utf-8'))
                shutil.copyfileobj(file_in, file_out)
        logging.info("Extraction complete")

    logging.info("Creating tarball")
    shutil.make_archive('targeted_assertions', 'gztar', root_dir=dir)


def export_kg(session: sqlalchemy.orm.Session, bucket: str, blob_prefix: str, use_uniprot: bool=False, edge_limit: int=0) -> None: # pragma: no cover
    """
    Create and upload the node and edge KGX files for targeted assertions.

    :param session: the database session
    :param bucket: the output GCP bucket name
    :param blob_prefix: the directory prefix for the uploaded files
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :param edge_limit: the maximum number of supporting study results per edge to include in the JSON blob (0 is no limit)
    """
    (node_curies, normal_dict) = get_node_data(session, use_uniprot=use_uniprot)
    node_metadata = write_nodes(node_curies, normal_dict, "nodes.tsv.gz")
    services.upload_to_gcp(bucket, 'nodes.tsv.gz', f"{blob_prefix}nodes.tsv.gz")
    edge_metadata = write_edges(session, normal_dict, "edges.tsv.gz", use_uniprot=use_uniprot, limit=edge_limit)
    services.upload_to_gcp(bucket, 'edges.tsv.gz', f"{blob_prefix}edges.tsv.gz")
    create_kge_tarball('tmp', node_metadata, edge_metadata)
    services.upload_to_gcp(bucket, 'targeted_assertions.tar.gz', f"{blob_prefix}targeted_assertions.tar.gz")
