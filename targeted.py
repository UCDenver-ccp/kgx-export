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
    curies = list(set(curies))
    logging.info(f'node curies retrieved and uniquified ({len(curies)})')
    if len(curies) > 10000:
        normalized_nodes = services.get_normalized_nodes_by_parts(curies)
    else:
        normalized_nodes = services.get_normalized_nodes(curies)
    node_list = []
    for assertion in assertions:
        node_list.extend(assertion.get_node_kgx(normalized_nodes))
    logging.info(f'full node list created ({len(node_list)})')
    unique_curies = set([])
    nodes = []
    for node in node_list:
        if node[0] not in unique_curies:
            unique_curies.add(node[0])
            nodes.append(node)
    logging.info(f'unique node list created ({len(nodes)})')
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
    curies = list(set(curies))
    logging.info(f'"other" node curies retrieved and uniquified ({len(curies)})')
    if len(curies) > 10000:
        normalized_nodes = services.get_normalized_nodes_by_parts(curies)
    else:
        normalized_nodes = services.get_normalized_nodes(curies)
    node_list = []
    for assertion in assertions:
        node_list.extend(assertion.get_uniprot_node_kgx(normalized_nodes))
    logging.info(f'full node list created ({len(node_list)})')
    unique_curies = set([])
    nodes = []
    for node in node_list:
        if node[0] not in unique_curies:
            unique_curies.add(node[0])
            nodes.append(node)
    logging.info(f'unique node list created ({len(nodes)})')
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
    logging.info(f'got edges ({len(edges)})')
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
    logging.info(f'got "other" edges ({len(other_edges)})')
    with open(output_filename, 'w') as outfile:
        for edge in other_edges:
            if len(edge) == 0:
                continue
            outfile.write('\t'.join(str(val) for val in edge))
            outfile.write('\n')
    logging.info('edge output complete')


def export_all(session):
    evaluation_subquery = session.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    correct_assertions = session.query(models.Assertion) \
        .options(joinedload(models.Assertion.evidence_list)) \
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))
    logging.info(f"got assertions ({correct_assertions.count()})")
    get_nodes(correct_assertions, "nodes.tsv")
    get_uniprot_nodes(correct_assertions, "nodes_uniprot.tsv")
    get_edges(correct_assertions, "edges.tsv")
    get_other_edges(correct_assertions, "edges_uniprot.tsv")
    logging.info("Files generated Main")
