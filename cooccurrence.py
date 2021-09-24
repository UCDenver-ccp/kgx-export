import logging
import argparse
import os
import sqlalchemy
import models
import services


def get_kgx_nodes(curies, normalized_nodes) -> list:
    for curie in curies:
        name = 'UNKNOWN_NAME'
        category = 'biolink:NamedThing'
        if curie in normalized_nodes and normalized_nodes[curie] is not None:
            name = normalized_nodes[curie]['id']['label'] if 'label' in normalized_nodes[curie]['id'] else curie
            category = 'biolink:ChemicalEntity' if curie.startswith('CHEBI') else 'biolink:Protein'
        yield [curie, name, category]


def write_nodes(session, outfile, use_uniprot=False) -> None:
    # TODO: find out how to page the query in case there is an extreme amount of data
    curie_list = []
    if use_uniprot:
        entity1_rows = session.query(sqlalchemy.text('entity1_curie, uniprot FROM cooccurrence LEFT JOIN pr_to_uniprot ON entity1_curie = pr'))
        entity2_rows = session.query(sqlalchemy.text('entity2_curie, uniprot FROM cooccurrence LEFT JOIN pr_to_uniprot ON entity2_curie = pr'))
        for row in entity1_rows:
            if row[0].startswith('PR:') and len(row) == 1:
                continue
            curie_list.append(row[1] if len(row) > 1 and row[1] else row[0])
        for row in entity2_rows:
            if row[0].startswith('PR:') and len(row) == 1:
                continue
            curie_list.append(row[1] if len(row) > 1 and row[1] else row[0])
    else:
        curie_list = [row[0] for row in session.query(sqlalchemy.text('DISTINCT entity1_curie FROM cooccurrence')).all()]
        curie_list.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT entity2_curie FROM cooccurrence')).all()])
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


def write_edges(session, outfile, use_uniprot=False) -> None:
    cooccurrence_list = session.query(models.Cooccurrence).all()
    logging.info(f'cooccurrence list generated ({len(cooccurrence_list)})')
    with open(outfile, 'w') as output:
        for cooccurrence in cooccurrence_list:
            output.write('\t'.join(cooccurrence.get_edge_kgx(use_uniprot=use_uniprot)) + '\n')
    logging.info('File written')


def export_all(session):
    write_nodes(session, 'c_nodes.tsv')
    write_edges(session, 'c_edges.tsv')
    logging.info('PR files written')
    write_nodes(session, 'cu_nodes.tsv', use_uniprot=True)
    write_edges(session, 'cu_edges.tsv', use_uniprot=True)
    logging.info("UniProt files written")
