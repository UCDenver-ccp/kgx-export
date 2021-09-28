import sqlalchemy
import os
import models
import services
import argparse
import logging
from sqlalchemy.orm import joinedload
from google.cloud import storage
from datetime import datetime, timezone
import math

ASSERTION_LIMIT = 50000


def get_kgx_nodes(curies, normalized_nodes) -> list:
    for curie in curies:
        name = 'UNKNOWN_NAME'
        category = 'biolink:NamedThing'
        if curie in normalized_nodes and normalized_nodes[curie] is not None:
            name = normalized_nodes[curie]['id']['label'] if 'label' in normalized_nodes[curie]['id'] else curie
            category = normalized_nodes[curie]['type'][0]
        yield [curie, name, category]


def get_nodes(session, output_filename) -> None:
    """
    Get the subject and object curies from assertions, normalize and uniquify them, and then output to a TSV file.

    :param output_filename: filepath for the output TSV file in KGX format.
    """
    curies = [row[0] for row in session.query(sqlalchemy.text('DISTINCT subject_curie FROM assertion')).all()]
    curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT object_curie FROM assertion')).all()])
    curies = list(set(curies))
    logging.info(f'node curies retrieved and uniquified ({len(curies)})')
    if len(curies) > 10000:
        normalized_nodes = services.get_normalized_nodes_by_parts(curies)
    else:
        normalized_nodes = services.get_normalized_nodes(curies)
    with open(output_filename, 'w') as outfile:
        for node in get_kgx_nodes(curies, normalized_nodes):
            outfile.write('\t'.join(node) + '\n')
    logging.info('node output complete')


def get_uniprot_nodes(session, output_filename) -> None:
    """
    Get the subject and object curies from assertions (using the UniProt names where available),
    normalize and uniquify them, and then output to a TSV file in KGX format.

    :param output_filename: filepath for the output TSV file.
    """
    curies = [row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, subject_curie) as curie FROM assertion LEFT JOIN pr_to_uniprot ON subject_curie = pr')).all()]
    curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, object_curie) as curie FROM assertion LEFT JOIN pr_to_uniprot ON object_curie = pr')).all()])
    curies = list(set(curies))
    logging.info(f'translated node curies retrieved ({len(curies)})')
    curies = [curie for curie in curies if not curie.startswith('PR:')]
    logging.info(f'removed PR curies, leaving {len(curies)}')
    if len(curies) > 10000:
        normalized_nodes = services.get_normalized_nodes_by_parts(curies)
    else:
        normalized_nodes = services.get_normalized_nodes(curies)
    with open(output_filename, 'w') as outfile:
        for node in get_kgx_nodes(curies, normalized_nodes):
            outfile.write('\t'.join(node) + '\n')
    logging.info('node output complete')


def get_edges(session, file_prefix, bucket, directory, pr=True):
    """
    Get the edge (or edges) associated with each assertion and output them to a TSV file in KGX edge format.

    :param session: the SQLAlchemy session to use to query the database
    :param output_filename: filepath for the output TSV file
    """
    evaluation_subquery = session.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    assertion_count = session.query(models.Assertion).filter(models.Assertion.assertion_id.notin_(evaluation_subquery)).count()
    file_count = math.ceil(assertion_count / ASSERTION_LIMIT)
    logging.info(f"Total Assertions: {assertion_count}")
    logging.info(f"Total File Count: {file_count}")
    logging.info(f"Mode: {'PR' if pr else 'UniProt'}")
    assertion_query = sqlalchemy.select(models.Assertion)\
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))\
        .execution_options(stream_results=True)
    x = 0
    for file_num in range(0,file_count):
        with open(f"{file_prefix}{file_num}.tsv", "w") as outfile:
            for assertion, in session.execute(assertion_query.offset(file_num * ASSERTION_LIMIT).limit(ASSERTION_LIMIT)):
                x += 1
                if x % (ASSERTION_LIMIT / 5) == 0:
                    logging.debug(f'Assertion count: {x}')
                if pr:
                    edges = assertion.get_edges_kgx()
                else:
                    edges = assertion.get_other_edges_kgx()
                for edge in edges:
                    if len(edge) == 0:
                        continue
                    outfile.write('\t'.join(str(val) for val in edge))
                    outfile.write('\n')
        logging.info(f"Done writing file {file_prefix}{file_num}.tsv")
        services.upload_to_gcp(bucket, f"{file_prefix}{file_num}.tsv", f"{directory}{file_prefix}{file_num}.tsv")
    services.compose_gcp_files(bucket, directory, file_prefix, "edges.tsv")
    services.remove_temp_files(bucket, [f"{file_prefix}{num}.tsv" for num in range(0, file_count)])
    logging.info(f"{'PR' if pr else 'UniProt'} edge output complete")


def export_all(session, pr_bucket, uniprot_bucket, ontology=None):
    logging.info("Starting export of Targeted Assertions")
    if (ontology and (ontology.lower() == 'uniprot' or ontology.lower() == 'both')) or not ontology:
        get_uniprot_nodes(session, "nodes_uniprot.tsv")
        services.upload_to_gcp(uniprot_bucket, 'nodes_uniprot.tsv', 'kgx/UniProt/nodes.tsv')
        get_edges(session, "edges", uniprot_bucket, "kgx/UniProt/", False)
    if (ontology and (ontology.lower() == 'pr' or ontology.lower() == 'both')) or not ontology:
        get_nodes(session, "nodes.tsv")
        services.upload_to_gcp(pr_bucket, 'nodes.tsv', 'kgx/PR/nodes.tsv')
        get_edges(session, "edges", pr_bucket, "kgx/PR/", True)
    logging.info("Targeted Assertions export complete")
