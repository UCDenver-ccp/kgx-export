import sqlalchemy
import os
import models
import services
import argparse
import logging
from sqlalchemy.orm import joinedload
from google.cloud import storage
from datetime import datetime, timezone


def get_nodes(assertions, output_filename) -> None:
    """
    Get the subject and object curies from assertions, normalize and uniquify them, and then output to a TSV file.

    :param assertions: the list of Assertion objects to pull the curies from
    :param output_filename: filepath for the output TSV file in KGX format.
    """
    curies = [assertion.object_curie for assertion in assertions]
    curies.extend([assertion.subject_curie for assertion in assertions])
    logging.info('node curies retrieved')
    normalized_nodes = services.get_normalized_nodes(curies)
    node_list = []
    for assertion in assertions:
        node_list.extend(assertion.get_node_kgx(normalized_nodes))
    logging.info('full node list created')
    unique_curies = set([])
    nodes = []
    for node in node_list:
        if node[0] not in unique_curies:
            unique_curies.add(node[0])
            nodes.append(node)
    logging.info('unique node list created')
    with open(output_filename, 'w') as outfile:
        for node in nodes:
            outfile.write('\t'.join(str(val) for val in node))
            outfile.write('\n')
    logging.info('node output complete')


def get_uniprot_nodes(assertions, output_filename) -> None:
    """
    Get the subject and object curies from assertions (using the UniProt names where available),
    normalize and uniquify them, and then output to a TSV file in KGX format.

    :param assertions: the list of Assertion objects to pull the curies from
    :param output_filename: filepath for the output TSV file.
    """
    curies = [(assertion.object_uniprot.uniprot if assertion.object_uniprot else assertion.object_curie) for assertion in assertions]
    curies.extend([assertion.subject_uniprot.uniprot if assertion.subject_uniprot else assertion.subject_curie for assertion in assertions])
    logging.info('"other" node curies retrieved')
    normalized_nodes = services.get_normalized_nodes(curies)
    node_list = []
    for assertion in assertions:
        node_list.extend(assertion.get_uniprot_node_kgx(normalized_nodes))
    logging.info('full node list created')
    unique_curies = set([])
    nodes = []
    for node in node_list:
        if node[0] not in unique_curies:
            unique_curies.add(node[0])
            nodes.append(node)
    logging.info('unique node list created')
    with open(output_filename, 'w') as outfile:
        for node in nodes:
            outfile.write('\t'.join(str(val) for val in node))
            outfile.write('\n')
    logging.info('node output complete')


def get_edges(assertions, output_filename):
    """
    Get the edge (or edges) associated with each assertion and output them to a TSV file in KGX edge format.

    :param assertions: the list of Assertion objects to get the edges from
    :param output_filename: filepath for the output TSV file
    """
    edges = []
    for assertion in assertions:
        edges.extend(assertion.get_edges_kgx())
    logging.info('got edges')
    with open(output_filename, 'w') as outfile:
        for edge in edges:
            outfile.write('\t'.join(str(val) for val in edge))
            outfile.write('\n')
    logging.info('edge output complete')


def get_other_edges(assertions, output_filename):
    """
    Get the edge (or edges) associated with each assertion and output them to a TSV file in KGX edge format.
    This version favors the UniProt names, where available, for subject and object.

    :param assertions: the list of Assertion objects to get the edges from
    :param output_filename: filepath for the output TSV file
    """
    other_edges = []
    for assertion in assertions:
        other_edges.extend(assertion.get_other_edges_kgx())
    logging.info('got "other" edges')
    with open(output_filename, 'w') as outfile:
        for edge in other_edges:
            if len(edge) == 0:
                continue
            outfile.write('\t'.join(str(val) for val in edge))
            outfile.write('\n')
    logging.info('edge output complete')


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


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(funcName)s: %(message)s', datefmt='%Y-%m-%d_%H%M%S', level=logging.INFO)
    logging.debug('starting main')
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ip', help='database IP address')
    parser.add_argument('-d', '--database', help='database name')
    parser.add_argument('-u', '--user', help='database username')
    parser.add_argument('-p', '--password', help='database password')
    # parser.add_argument('-x', '--instance', help='GCP DB instance name')
    parser.add_argument('-pr', help='storage bucket for PR data')
    parser.add_argument('-uni', help='storage bucket for UniProt data')
    args = parser.parse_args()
    pr_bucket = args.pr if args.pr else 'test_kgx_output_bucket'
    uniprot_bucket = args.uni if args.uni else 'test_kgx_output_bucket'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'kgx-creds.json'
    models.init_db(args)
    s = models.session()
    evaluation_subquery = s.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    correct_assertions = s.query(models.Assertion) \
        .options(joinedload(models.Assertion.evidence_list)) \
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))
    logging.info("Got Assertions Main")
    get_nodes(correct_assertions, f"out\\nodes.tsv")
    get_uniprot_nodes(correct_assertions, f"out\\nodes_uniprot.tsv")
    get_edges(correct_assertions, f"out\\edges.tsv")
    get_other_edges(correct_assertions, f"out\\edges_uniprot.tsv")
    logging.info("Files generated Main")
    file_suffix_timestamp = datetime.now(timezone.utc).strftime("_%Y-%m-%d")
    upload_to_gcp(pr_bucket, 'out\\nodes.tsv', 'kgx/PR/nodes.tsv')
    upload_to_gcp(uniprot_bucket, 'out\\nodes_uniprot.tsv', 'kgx/UniProt/nodes.tsv')
    upload_to_gcp(pr_bucket, 'out\\edges.tsv', 'kgx/PR/edges.tsv')
    upload_to_gcp(uniprot_bucket, 'out\\edges_uniprot.tsv', 'kgx/UniProt/edges.tsv')
    logging.info("Files uploaded Main")
    logging.info("End Main")
