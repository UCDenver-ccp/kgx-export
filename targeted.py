import gzip
import logging
import math
from typing import Iterator

import models
import services
import sqlalchemy

ROW_BATCH_SIZE = 50000


def get_kgx_nodes(curies: list[str], normalized_nodes:dict[str, dict]) -> Iterator[list[str]]:
    """
    Get the KGX node representation of a curie

    :param curies: the list of curies to turn into KGX nodes
    :param normalized_nodes: a dictionary of normalized nodes, for retrieving canonical label and category
    """
    for curie in curies:
        name = 'UNKNOWN_NAME'
        category = 'biolink:NamedThing'
        if curie in normalized_nodes and normalized_nodes[curie] is not None:
            name = normalized_nodes[curie]['id']['label'] if 'label' in normalized_nodes[curie]['id'] else curie
            category = normalized_nodes[curie]['type'][0]
        yield [curie, name, category]


def write_nodes_compressed(session: sqlalchemy.orm.Session, output_filename: str, use_uniprot: bool=False) -> None:
    """
    Get the subject and object curies from assertions, normalize and uniquify them, and then output to a gzipped TSV file according to KGX node format.

    :param session: the database session.
    :param output_filename: filepath for the output file.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    """
    logging.info("Starting node output")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    if use_uniprot:
        curies = [row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, subject_curie) as curie FROM assertion LEFT JOIN pr_to_uniprot ON subject_curie = pr')).all()]
        curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, object_curie) as curie FROM assertion LEFT JOIN pr_to_uniprot ON object_curie = pr')).all()])
    else:
        curies = [row[0] for row in session.query(sqlalchemy.text('DISTINCT subject_curie FROM assertion')).all()]
        curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT object_curie FROM assertion')).all()])
    curies = list(set(curies))
    logging.debug(f'node curies retrieved and uniquified ({len(curies)})')
    if use_uniprot:
        curies = [curie for curie in curies if not curie.startswith('PR:')]
    if len(curies) > 10000:
        normalized_nodes = services.get_normalized_nodes_by_parts(curies)
    else:
        normalized_nodes = services.get_normalized_nodes(curies)
    with gzip.open(output_filename, 'wb') as outfile:
        for node in get_kgx_nodes(curies, normalized_nodes):
            line = '\t'.join(node) + '\n'
            outfile.write(line.encode('utf-8'))
    logging.info('Node output complete')


def write_edges_compressed(session: sqlalchemy.orm.Session, output_filename: str, use_uniprot: bool=False, limit: int=0) -> None:
    """
    Get the edge (or edges) associated with each assertion and output them to a gzipped TSV file according to KGX edge format.

    :param session: the database session.
    :param output_filename: filepath for the output file.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :param limit: the maximum number of supporting study results per edge to include in the JSON blob (0 is no limit)
    """
    logging.info("Starting edge output")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    evaluation_subquery = session.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    assertion_count = session.query(models.Assertion).filter(models.Assertion.assertion_id.notin_(evaluation_subquery)).count()
    partition_count = math.ceil(assertion_count / ROW_BATCH_SIZE)
    logging.debug(f"Total Assertions: {assertion_count}")
    logging.debug(f"Total Partition Count: {partition_count}")
    assertion_query = sqlalchemy.select(models.Assertion)\
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))\
        .execution_options(stream_results=True)
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
                    line = '\t'.join(str(val) for val in edge) + '\n'
                    outfile.write(line.encode('utf-8'))
            outfile.flush()
            logging.debug(f"Done with partition {partition_number}")
    logging.info("Edge output complete")


def export_kg(session: sqlalchemy.orm.Session, bucket: str, blob_prefix: str, use_uniprot: bool=False, edge_limit: int=0) -> None:
    """
    Create and upload the node and edge KGX files for targeted assertions.

    :param session: the database session
    :param bucket: the output GCP bucket name
    :param blob_prefix: the directory prefix for the uploaded files
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :param edge_limit: the maximum number of supporting study results per edge to include in the JSON blob (0 is no limit)
    """
    write_nodes_compressed(session, "nodes.tsv.gz", use_uniprot=use_uniprot)
    services.upload_to_gcp(bucket, 'nodes.tsv.gz', f"{blob_prefix}nodes.tsv.gz")
    write_edges_compressed(session, "edges.tsv.gz", use_uniprot=use_uniprot, limit=edge_limit)
    services.upload_to_gcp(bucket, 'edges.tsv.gz', f"{blob_prefix}edges.tsv.gz")
