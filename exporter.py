import logging
import argparse
import os
import services
import models
import cooccurrence
import targeted


def upload_all(kg, pr_bucket, uniprot_bucket):
    if kg == 'targeted' or kg == 'all':
        services.upload_to_gcp(pr_bucket, 'nodes.tsv', 'kgx/PR/nodes.tsv')
        services.upload_to_gcp(uniprot_bucket, 'nodes_uniprot.tsv', 'kgx/UniProt/nodes.tsv')
        services.upload_to_gcp(pr_bucket, 'edges.tsv', 'kgx/PR/edges.tsv')
        services.upload_to_gcp(uniprot_bucket, 'edges_uniprot.tsv', 'kgx/UniProt/edges.tsv')
        logging.info('uploaded targeted files')
    if kg == 'cooccurrence' or kg == 'all':
        services.upload_to_gcp(pr_bucket, 'c_nodes.tsv', 'kgx/PR/cooccurrence_nodes.tsv')
        services.upload_to_gcp(uniprot_bucket, 'cu_nodes.tsv', 'kgx/UniProt/cooccurrence_nodes.tsv')
        services.upload_to_gcp(pr_bucket, 'c_edges.tsv', 'kgx/PR/cooccurrence_edges.tsv')
        services.upload_to_gcp(uniprot_bucket, 'cu_edges.tsv', 'kgx/UniProt/cooccurrence_edges.tsv')
        logging.info('uploaded cooccurrence files')


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(module)s:%(funcName)s:%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('starting main')
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--knowledgegraph', help='the knowledge graph to export, or all')
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
        cooccurrence.export_all(session)
    if args.knowledgegraph == 'targeted' or args.knowledgegraph == 'all':
        targeted.export_all(session)
    upload_all(args.knowledgegraph, pr_bucket, uniprot_bucket)
    logging.info("Files uploaded Main")
    logging.info("End Main")
