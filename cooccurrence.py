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
            category = 'biolink:ChemicalEntity' if curie.startswith('CHEBI') else 'biolink:Protein'
        yield [curie, name, category]


def write_nodes_compressed(session: sqlalchemy.orm.Session, outfile: str, use_uniprot: bool=False) -> None:
    """
    Get the subject and object curies from cooccurrences, normalize and uniquify them, and then output to a gzipped TSV file according to KGX node format.

    :param session: the database session.
    :param output_filename: filepath for the output file.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    """
    logging.info("Starting node export")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    curies = []
    curies = []
    if use_uniprot:
        entity1_curie_query = sqlalchemy.select(sqlalchemy.text(
            """
            IFNULL(uniprot, entity1_curie) as curie 
            FROM cooccurrence 
                LEFT JOIN pr_to_uniprot ON entity1_curie = pr
            """
        ))
    else:
        entity1_curie_query = sqlalchemy.select(sqlalchemy.text(
            """entity1_curie FROM cooccurrence"""
        ))

    for curie, in session.execute(entity1_curie_query.execution_options(stream_results=True)):
        curies.append(curie)
        if len(curies) % 50000 == 0:
            curies = list(set(curies))
    logging.debug(f'Got unique entity1 curies ({len(curies)})')
    if use_uniprot:
        entity2_curie_query = sqlalchemy.select(sqlalchemy.text(
            """
            IFNULL(uniprot, entity2_curie) as curie 
            FROM cooccurrence 
            LEFT JOIN pr_to_uniprot ON entity2_curie = pr
            """
        ))
    else:
        entity2_curie_query = sqlalchemy.select(sqlalchemy.text(
            """entity2_curie FROM cooccurrence"""
        ))
    for curie, in session.execute(entity2_curie_query.execution_options(stream_results=True)):
        curies.append(curie)
        if len(curies) % 50000 == 0:
            curies = list(set(curies))
    curies = list(set(curies))
    logging.debug(f'unique node curies retrieved and uniquified ({len(curies)})')
    if use_uniprot:
        curies = [curie for curie in curies if not curie.startswith('PR:')]
    if len(curies) > 10000:
        normalized_nodes = services.get_normalized_nodes_by_parts(curies, sublist_size=5000)
    else:
        normalized_nodes = services.get_normalized_nodes(curies)
    with gzip.open(outfile, 'wb') as output:
        for node in get_kgx_nodes(curies, normalized_nodes):
            line = '\t'.join(node) + '\n'
            output.write(line.encode('utf-8'))
    logging.info('Node output complete')


def write_edges_compressed(session: sqlalchemy.orm.Session, output_filename: str, use_uniprot: bool=False) -> None:
    """
    Get the edge (or edges) associated with each assertion and output them to a gzipped TSV file according to KGX edge format.

    :param session: the database session.
    :param output_filename: filepath for the output file.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    """
    logging.info('Starting edge output')
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    cooccurrence_count = session.query(models.Cooccurrence).count()
    partition_count = math.ceil(cooccurrence_count / ROW_BATCH_SIZE)
    logging.debug(f"Total Cooccurrence Records: {cooccurrence_count}")
    logging.debug(f"Total Partition Count: {partition_count}")
    cooccurrence_query = sqlalchemy.select(models.Cooccurrence).execution_options(stream_results=True)
    with gzip.open(output_filename, "wb") as outfile:
        for partition_number in range(0, partition_count):
            for cooccurrence, in session.execute(cooccurrence_query.offset(partition_number * ROW_BATCH_SIZE).limit(ROW_BATCH_SIZE)):
                line = '\t'.join(cooccurrence.get_edge_kgx(use_uniprot=use_uniprot)) + '\n'
                outfile.write(line.encode('utf-8'))
            outfile.flush()
            logging.debug(f"Done with partition {partition_number}")
    logging.info('Edge output complete')


def export_kg(session: sqlalchemy.orm.Session, bucket: str, blob_prefix: str, use_uniprot: bool=False) -> None:
    """
    Create and upload the node and edge KGX files for targeted assertions.

    :param session: the database session
    :param bucket: the output GCP bucket name
    :param blob_prefix: the directory prefix for the uploaded files
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    """
    write_nodes_compressed(session, 'nodes.tsv.gz', use_uniprot=use_uniprot)
    services.upload_to_gcp(bucket, 'nodes.tsv.gz', f'{blob_prefix}cooccurrence_nodes.tsv.gz')
    write_edges_compressed(session, "edges.tsv.gz", use_uniprot=use_uniprot)
    services.upload_to_gcp(bucket, 'edges.tsv.gz', f'{blob_prefix}cooccurrence_edges.tsv.gz')
