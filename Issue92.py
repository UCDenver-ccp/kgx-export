import sqlalchemy
import os
import sys
import models
import services
import urllib.parse
from sqlalchemy.orm import joinedload
from google.cloud import storage
from datetime import datetime, timezone


def get_nodes(assertions, output_filename):
    """
    Get the subject and object curies from assertions, normalize and uniquify them, and then output to a TSV file.

    :param assertions: the list of Assertion objects to pull the curies from
    :param output_filename: filepath for the output TSV file in KGX format.
    """
    services.log_timestamp("Start GN")
    curies = [assertion.object_curie for assertion in assertions]
    services.log_timestamp("Got Curies 1 GN")
    curies.extend([assertion.subject_curie for assertion in assertions])
    services.log_timestamp("Got Curies 2 GN")
    normalized_nodes = services.get_normalized_nodes(curies)
    node_list = []
    for assertion in assertions:
        node_list.extend(assertion.get_node_kgx(normalized_nodes))
    services.log_timestamp("Got node_list GN")
    unique_curies = set([])
    nodes = []
    for node in node_list:
        if node[0] not in unique_curies:
            unique_curies.add(node[0])
            nodes.append(node)
    services.log_timestamp("Got nodes GN")
    with open(output_filename, 'w') as outfile:
        for node in nodes:
            outfile.write('\t'.join(str(val) for val in node))
            outfile.write('\n')
    services.log_timestamp("End GN")


def get_uniprot_nodes(assertions, output_filename):
    """
    Get the subject and object curies from assertions (using the UniProt names where available),
    normalize and uniquify them, and then output to a TSV file in KGX format.

    :param assertions: the list of Assertion objects to pull the curies from
    :param output_filename: filepath for the output TSV file.
    """
    services.log_timestamp("Start GON")
    curies = [(assertion.object_uniprot.uniprot if assertion.object_uniprot else assertion.object_curie) for assertion in assertions]
    services.log_timestamp("Got Curies 1 GON")
    curies.extend([assertion.subject_uniprot.uniprot if assertion.subject_uniprot else assertion.subject_curie for assertion in assertions])
    services.log_timestamp("Got Curies 2 GON")
    normalized_nodes = services.get_normalized_nodes(curies)
    node_list = []
    for assertion in assertions:
        node_list.extend(assertion.get_uniprot_node_kgx(normalized_nodes))
    services.log_timestamp("Got node_list GON")
    unique_curies = set([])
    nodes = []
    for node in node_list:
        if node[0] not in unique_curies:
            unique_curies.add(node[0])
            nodes.append(node)
    services.log_timestamp("Got nodes GON")
    with open(output_filename, 'w') as outfile:
        for node in nodes:
            outfile.write('\t'.join(str(val) for val in node))
            outfile.write('\n')
    services.log_timestamp("End GON")


def get_edges(assertions, output_filename):
    """
    Get the edge (or edges) associated with each assertion and output them to a TSV file in KGX edge format.

    :param assertions: the list of Assertion objects to get the edges from
    :param output_filename: filepath for the output TSV file
    """
    services.log_timestamp("Start GE")
    edges = []
    for assertion in assertions:
        edges.extend(assertion.get_edges_kgx())
    services.log_timestamp("Got edges GE")
    with open(output_filename, 'w') as outfile:
        for edge in edges:
            outfile.write('\t'.join(str(val) for val in edge))
            outfile.write('\n')
    services.log_timestamp("Wrote edges GE")
    services.log_timestamp("End GE")


def get_other_edges(assertions, output_filename):
    """
    Get the edge (or edges) associated with each assertion and output them to a TSV file in KGX edge format.
    This version favors the UniProt names, where available, for subject and object.

    :param assertions: the list of Assertion objects to get the edges from
    :param output_filename: filepath for the output TSV file
    """
    services.log_timestamp("Start GE")
    other_edges = []
    for assertion in assertions:
        other_edges.extend(assertion.get_other_edges_kgx())
    services.log_timestamp("Got other edges GE")
    with open(output_filename, 'w') as outfile:
        for edge in other_edges:
            outfile.write('\t'.join(str(val) for val in edge))
            outfile.write('\n')
    services.log_timestamp("Wrote other edges GE")
    services.log_timestamp("End GE")


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
    blob.upload_from_filename(source_file_name)


if __name__ == "__main__":
    services.log_timestamp("Start Main")
    pr_bucket = 'test_kgx_output_bucket'
    uniprot_bucket = 'test_kgx_output_bucket'
    # TODO: Handle command line parameters better (e.g. with argparse)
    if len(sys.argv) > 1:
        connection_string = sys.argv[1]
        models.init_db(url=connection_string)
        if len(sys.argv) == 4:
            pr_bucket = sys.argv[2]
            uniprot_bucket = sys.argv[3]
    else:
        username = os.getenv('MYSQL_DATABASE_USER', None)
        secret_password = os.getenv('MYSQL_DATABASE_PASSWORD', None)
        assert username
        assert secret_password
        models.init_db(username=username, password=urllib.parse.quote_plus(secret_password))
    s = models.session()
    evaluation_subquery = s.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    correct_assertions = s.query(models.Assertion) \
        .options(joinedload(models.Assertion.evidence_list)) \
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))
    services.log_timestamp("Got Assertions Main")
    get_nodes(correct_assertions, f"out\\nodes.tsv")
    get_uniprot_nodes(correct_assertions, f"out\\nodes_uniprot.tsv")
    get_edges(correct_assertions, f"out\\edges.tsv")
    get_other_edges(correct_assertions, f"out\\edges_uniprot.tsv")
    services.log_timestamp("Files generated Main")
    file_suffix_timestamp = datetime.now(timezone.utc).strftime("_%Y-%m-%d_%H%M%S")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'creds.json'
    upload_to_gcp(pr_bucket, 'out\\nodes.tsv', f'nodes{file_suffix_timestamp}.tsv')
    upload_to_gcp(uniprot_bucket, 'out\\nodes_uniprot.tsv', f'nodes_uniprot{file_suffix_timestamp}.tsv')
    upload_to_gcp(pr_bucket, 'out\\edges.tsv', f'edges{file_suffix_timestamp}.tsv')
    upload_to_gcp(uniprot_bucket, 'out\\edges_uniprot.tsv', f'edges_uniprot{file_suffix_timestamp}.tsv')
    services.log_timestamp("Files uploaded Main")
    services.log_timestamp("End Main")
