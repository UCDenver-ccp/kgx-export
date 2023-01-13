import gzip
import json
import logging
import math
import os
import shutil
from typing import Union, Any

import sqlalchemy

import models
import services

ROW_BATCH_SIZE = 40000
HUMAN_TAXON = 'NCBITaxon:9606'
ORIGINAL_KNOWLEDGE_SOURCE = "infores:text-mining-provider-targeted"


def get_node_data(session: sqlalchemy.orm.Session, use_uniprot: bool = False) -> (list[str], dict[str, dict]):
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
        for node in services.get_kgx_nodes(curies, normalize_dict):
            if len(node) == 0:
                continue
            line = '\t'.join(node) + '\n'
            outfile.write(line.encode('utf-8'))
            metadata_dict = services.update_node_metadata(node, metadata_dict, ORIGINAL_KNOWLEDGE_SOURCE)
    logging.info('Node output complete')
    return metadata_dict


def write_edges(session: sqlalchemy.orm.Session, normalize_dict: dict[str, dict], output_filename: str, use_uniprot: bool = False, limit: int = 0) -> Union[dict[Any, Any], dict]:
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
                    if not (services.is_normal(edge[0], normalize_dict) and services.is_normal(edge[2], normalize_dict)):
                        continue
                    line = '\t'.join(str(val) for val in edge) + '\n'
                    outfile.write(line.encode('utf-8'))
                    metadata_dict = services.update_edge_metadata(edge, metadata_dict, normalize_dict, ORIGINAL_KNOWLEDGE_SOURCE)
            outfile.flush()
            logging.info(f"Done with partition {partition_number}")
    logging.info("Edge output complete")
    return metadata_dict


def create_kge_tarball(directory: str, node_metadata: dict, edge_metadata: dict):
    logging.info("Starting KGE tarball creation")
    if not os.path.isdir(directory):
        os.mkdir(directory)
    node_file = os.path.join(directory, "nodes.tsv")
    edge_file = os.path.join(directory, "edges.tsv")
    metadata_file = os.path.join(directory, "content_metadata.json")

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
    shutil.make_archive('targeted_assertions', 'gztar', root_dir=directory)


def export_kg(session: sqlalchemy.orm.Session, bucket: str, blob_prefix: str, use_uniprot: bool = False, edge_limit: int = 0) -> None:  # pragma: no cover
    """
    Create and upload the node and edge KGX files for targeted assertions.

    :param session: the database session
    :param bucket: the output GCP bucket name
    :param blob_prefix: the directory prefix for the uploaded files
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :param edge_limit: the maximum number of supporting study results per edge to include in the JSON blob (0 is no limit)
    """
    (node_curies, normal_dict) = get_node_data(session, use_uniprot=use_uniprot)
    node_metadata = write_nodes(node_curies, normal_dict, 'nodes.tsv.gz')
    services.upload_to_gcp(bucket, 'nodes.tsv.gz', f'{blob_prefix}nodes.tsv.gz')
    edge_metadata = write_edges(session, normal_dict, "edges.tsv.gz", use_uniprot=use_uniprot, limit=edge_limit)
    services.upload_to_gcp(bucket, 'edges.tsv.gz', f'{blob_prefix}edges.tsv.gz')
    create_kge_tarball('tmp', node_metadata, edge_metadata)
    services.upload_to_gcp(bucket, 'targeted_assertions.tar.gz', f'{blob_prefix}targeted_assertions.tar.gz')
