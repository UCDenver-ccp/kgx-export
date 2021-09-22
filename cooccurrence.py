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
            name = normalized_nodes[curie]['id']['label']
            category = 'biolink:ChemicalEntity' if curie.startswith('CHEBI') else 'biolink:Protein'
        yield [curie, name, category]


def write_nodes(outfile, use_uniprot=False) -> None:
    # TODO: find out how to page the query in case there is an extreme amount of data
    curie_list = []
    if use_uniprot:
        entity1_rows = s.query(sqlalchemy.text('entity1_curie, uniprot FROM cooccurrence LEFT JOIN pr_to_uniprot ON entity1_curie = pr'))
        entity2_rows = s.query(sqlalchemy.text('entity2_curie, uniprot FROM cooccurrence LEFT JOIN pr_to_uniprot ON entity2_curie = pr'))
        for row in entity1_rows:
            if row[0].startswith('PR:') and len(row) == 1:
                continue
            curie_list.append(row[1] if len(row) > 1 and row[1] else row[0])
        for row in entity2_rows:
            if row[0].startswith('PR:') and len(row) == 1:
                continue
            curie_list.append(row[1] if len(row) > 1 and row[1] else row[0])
    else:
        curie_list = [row[0] for row in s.query(sqlalchemy.text('DISTINCT entity1_curie FROM cooccurrence')).all()]
        curie_list.extend([row[0] for row in s.query(sqlalchemy.text('DISTINCT entity2_curie FROM cooccurrence')).all()])
    logging.info('node curies retrieved')
    curie_list = list(set(curie_list))
    normalized_nodes = services.get_normalized_nodes(curie_list)
    with open(outfile, 'w') as output:
        for node in get_kgx_nodes(curie_list, normalized_nodes):
            output.write('\t'.join(node) + '\n')
    logging.info('File written')


def write_edges(outfile, use_uniprot=False) -> None:
    cooccurrence_list = s.query(models.Cooccurrence).all()
    logging.info('cooccurrence list generated')
    with open(outfile, 'w') as output:
        for cooccurrence in cooccurrence_list:
            output.write('\t'.join(cooccurrence.get_edge_kgx(use_uniprot=use_uniprot)) + '\n')
    logging.info('File written')


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(funcName)s: %(message)s', datefmt='%Y-%m-%d_%H%M%S', level=logging.INFO)
    logging.debug('starting main')
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ip', help='database IP address')
    parser.add_argument('-d', '--database', help='database name')
    parser.add_argument('-u', '--user', help='database username')
    parser.add_argument('-p', '--password', help='database password')
    parser.add_argument('-x', '--instance', help='GCP DB instance name')
    parser.add_argument('-pr', help='storage bucket for PR data')
    parser.add_argument('-uni', help='storage bucket for UniProt data')
    args = parser.parse_args()
    pr_bucket = args.pr if args.pr else 'test_kgx_output_bucket'
    uniprot_bucket = args.uni if args.uni else 'test_kgx_output_bucket'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'kgx-creds.json'
    models.init_db(args)
    s = models.session()
    write_nodes('out/c_nodes.tsv')
    write_edges('out/c_edges.tsv')
    logging.info('PR files written')
    write_nodes('out/cu_nodes.tsv', use_uniprot=True)
    write_edges('out/cu_edges.tsv', use_uniprot=True)
    logging.info("UniProt files written")
    services.upload_to_gcp(pr_bucket, 'out\\c_nodes.tsv', 'kgx/PR/cooccurrence_nodes.tsv')
    services.upload_to_gcp(uniprot_bucket, 'out\\cu_nodes.tsv', 'kgx/UniProt/cooccurrence_nodes.tsv')
    services.upload_to_gcp(pr_bucket, 'out\\c_edges.tsv', 'kgx/PR/cooccurrence_edges.tsv')
    services.upload_to_gcp(uniprot_bucket, 'out\\cu_edges.tsv', 'kgx/UniProt/cooccurrence_edges.tsv')
    logging.info("Files uploaded")
    logging.info("Fin")
