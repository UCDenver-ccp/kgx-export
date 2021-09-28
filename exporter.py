import logging
import argparse
import os
import services
import models
import cooccurrence
import targeted


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(module)s:%(funcName)s:%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('Starting Main')
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--knowledgegraph', help='the knowledge graph to export, or all')
    parser.add_argument('-o', '--ontology', help='the ontology to export, PR or UniProt, or both')
    parser.add_argument('-i', '--instance', help='GCP DB instance name')
    parser.add_argument('-d', '--database', help='database name')
    parser.add_argument('-u', '--user', help='database username')
    parser.add_argument('-p', '--password', help='database password')
    parser.add_argument('-pr', help='storage bucket for PR data')
    parser.add_argument('-uni', help='storage bucket for UniProt data')
    args = parser.parse_args()
    pr_bucket = args.pr if args.pr else 'test_kgx_output_bucket'
    uniprot_bucket = args.uni if args.uni else 'test_kgx_output_bucket'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'prod-creds.json'
    models.init_db(
        instance=args.instance if args.instance else os.getenv('MYSQL_DATABASE_INSTANCE', None),
        user=args.user if args.user else os.getenv('MYSQL_DATABASE_USER', None),
        password=args.password if args.password else os.getenv('MYSQL_DATABASE_PASSWORD', None),
        database=args.database if args.database else 'text_mined_assertions'
    )
    session = models.session()
    if args.knowledgegraph == 'cooccurrence' or args.knowledgegraph == 'all':
        if (args.ontology and (args.ontology.lower() == 'uniprot' or args.ontology.lower() == 'both')) or not args.ontology:
            cooccurrence.export_all(session, uniprot_bucket, 'kgx/UniProt/', use_uniprot=True)
        if (args.ontology and (args.ontology.lower() == 'pr' or args.ontology.lower() == 'both')) or not args.ontology:
            cooccurrence.export_all(session, pr_bucket, 'kgx/PR/', use_uniprot=False)
    if args.knowledgegraph == 'targeted' or args.knowledgegraph == 'all':
        targeted.export_all(session, pr_bucket, uniprot_bucket, args.ontology)
    logging.info("End Main")
