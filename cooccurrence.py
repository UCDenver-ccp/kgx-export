import logging
import argparse
import os
import sqlalchemy
import models
import services
import json

ROW_BATCH_SIZE = 50000


def get_kgx_nodes(curies, normalized_nodes) -> list:
    for curie in curies:
        name = 'UNKNOWN_NAME'
        category = 'biolink:NamedThing'
        if curie in normalized_nodes and normalized_nodes[curie] is not None:
            name = normalized_nodes[curie]['id']['label'] if 'label' in normalized_nodes[curie]['id'] else curie
            category = 'biolink:ChemicalEntity' if curie.startswith('CHEBI') else 'biolink:Protein'
        yield [curie, name, category]


def write_nodes(session, outfile, use_uniprot=False) -> None:
    curie_list = []
    if use_uniprot:
        unique_curies = []
        x = 0
        entity1_curie_query = sqlalchemy.select(sqlalchemy.text('IFNULL(uniprot, entity1_curie) as curie FROM cooccurrence LEFT JOIN pr_to_uniprot ON entity1_curie = pr')).execution_options(stream_results=True)
        for curie, in session.execute(entity1_curie_query):
            unique_curies.append(curie)
            x += 1
            if x % 100000 == 0:
                logging.debug(f'x = {x}. Uniquifying')
                unique_curies = list(set(unique_curies))
        logging.info(f'Got entity1 curies ({len(unique_curies)})')
        y = 0
        entity2_curie_query = sqlalchemy.select(sqlalchemy.text('IFNULL(uniprot, entity2_curie) as curie FROM cooccurrence LEFT JOIN pr_to_uniprot ON entity2_curie = pr')).execution_options(stream_results=True)
        for curie, in session.execute(entity2_curie_query):
            unique_curies.append(curie)
            y += 1
            if y % 100000 == 0:
                logging.debug(f'y = {y}. Uniquifying')
                unique_curies = list(set(unique_curies))
        curie_list = list(set(unique_curies))
    else:
        unique_curies = []
        x = 0
        entity1_curie_query = sqlalchemy.select(sqlalchemy.text('entity1_curie FROM cooccurrence')).execution_options(stream_results=True)
        for curie, in session.execute(entity1_curie_query):
            unique_curies.append(curie)
            x += 1
            if x % 100000 == 0:
                logging.debug(f'x = {x}. Uniquifying')
                unique_curies = list(set(unique_curies))
        logging.info(f'Got entity1 curies ({len(unique_curies)})')
        y = 0
        entity2_curie_query = sqlalchemy.select(sqlalchemy.text('entity2_curie FROM cooccurrence')).execution_options(stream_results=True)
        for curie, in session.execute(entity2_curie_query):
            unique_curies.append(curie)
            y += 1
            if y % 100000 == 0:
                logging.debug(f'y = {y}. Uniquifying')
                unique_curies = list(set(unique_curies))
        curie_list = list(set(unique_curies))
    logging.info(f'node curies retrieved ({len(curie_list)})')
    curie_list = list(set(curie_list))
    logging.info(f'unique node curies retrieved ({len(curie_list)})')
    if len(curie_list) > 10000:
        normalized_nodes = services.get_normalized_nodes_by_parts(curie_list)
    else:
        normalized_nodes = services.get_normalized_nodes(curie_list)
    with open(outfile, 'w') as output:
        for node in get_kgx_nodes(curie_list, normalized_nodes):
            output.write('\t'.join(node) + '\n')
    logging.info('File written')


def write_edges(session, file_prefix, bucket, directory, use_uniprot=False) -> None:
    cooccurrence_count = session.query(models.Cooccurrence).count()
    file_count = math.ceil(cooccurrence_count / ROW_BATCH_SIZE)
    logging.info(f"Total Cooccurrence Records: {cooccurrence_count}")
    logging.info(f"Total File Count: {file_count}")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    cooccurrence_query = sqlalchemy.select(models.Cooccurrence).execution_options(stream_results=True)
    x = 0
    for file_num in range(0,file_count):
        with open(f"{file_prefix}{file_num}.tsv", "w") as outfile:
            for cooccurrence, in session.execute(cooccurrence_query.offset(file_num * ROW_BATCH_SIZE).limit(ROW_BATCH_SIZE)):
                x += 1
                if x % (ASSERTION_LIMIT / 5) == 0:
                    logging.info(f'Cooccurrence count: {x}')
                output.write('\t'.join(cooccurrence.get_edge_kgx(use_uniprot=use_uniprot)) + '\n')
        logging.info(f"Done writing file {file_prefix}{file_num}.tsv")
        services.upload_to_gcp(bucket, f"{file_prefix}{file_num}.tsv", f"{directory}{file_prefix}{file_num}.tsv")
    services.compose_gcp_files(bucket, directory, file_prefix, "cooccurrence_edges.tsv")
    services.remove_temp_files(bucket, [f"{file_prefix}{num}.tsv" for num in range(0, file_count)])
    logging.info('edge output complete')


def export_all(session, bucket, blob_prefix, use_uniprot=True):
    write_nodes(session, 'nodes.tsv', use_uniprot)
    services.upload_to_gcp(bucket, 'c_nodes.tsv', f'{blob_prefix}cooccurrence_nodes.tsv')
    write_edges(session, 'c_edges', bucket, blob_prefix, use_uniprot)
