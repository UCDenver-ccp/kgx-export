import logging
import os

import argparse
import models
import targeted

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(module)s:%(funcName)s:%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('Starting Main')
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--ontology', help='the ontology to export, PR or UniProt, or both')
    parser.add_argument('-i', '--instance', help='GCP DB instance name')
    parser.add_argument('-d', '--database', help='database name')
    parser.add_argument('-u', '--user', help='database username')
    parser.add_argument('-p', '--password', help='database password')
    parser.add_argument('-pr', '--pr_bucket', help='storage bucket for PR data')
    parser.add_argument('-uni', '--uniprot_bucket', help='storage bucket for UniProt data')
    parser.add_argument('-c', '--chunk_size', help='number of assertions to process at a time', default=100, type=int)
    parser.add_argument('-l', '--limit', help='maximum number of publications to export per edge', default=0, type=int)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-e', '--kge_only', action='store_true')
    args = parser.parse_args()

    pr_bucket = args.pr_bucket if args.pr_bucket else 'test_kgx_output_bucket'
    uniprot_bucket = args.uniprot_bucket if args.uniprot_bucket else 'test_kgx_output_bucket'
    ontology = args.ontology.lower() if args.ontology else 'both'
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'prod-creds.json'
    models.init_db(
        instance=args.instance if args.instance else os.getenv('MYSQL_DATABASE_INSTANCE', None),
        user=args.user if args.user else os.getenv('MYSQL_DATABASE_USER', None),
        password=args.password if args.password else os.getenv('MYSQL_DATABASE_PASSWORD', None),
        database=args.database if args.database else 'text_mined_assertions'
    )
    session = models.session()

    logging.info("Exporting Targeted Assertion knowledge graph")
    if ontology == 'uniprot' or ontology == 'both':
        logging.info("Exporting UniProt")
        targeted.export_kg(session, uniprot_bucket, 'kgx/UniProt/', use_uniprot=True, chunk_size=args.chunk_size, edge_limit=args.limit)
    logging.info("End Main")
