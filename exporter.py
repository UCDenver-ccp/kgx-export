import logging
import os

import argparse
import models
import targeted
import services


def export_metadata(bucket):
    services.get_from_gcp(bucket, 'kgx/UniProt/edges.tsv', 'edges.tsv')
    services.get_from_gcp(bucket, 'kgx/UniProt/nodes.tsv.gz', 'nodes.tsv.gz')
    services.decompress('nodes.tsv.gz', 'nodes.tsv')
    services.generate_metadata('edges.tsv', 'nodes.tsv', 'KGE')
    services.compress('edges.tsv', 'edges.tsv.gz')
    services.upload_to_gcp(bucket, 'edges.tsv.gz', 'kgx/Test/edges.tsv.gz')
    services.upload_to_gcp(bucket, 'KGE/content_metadata.json', 'kgx/UniProt/content_metadata.json')
    services.upload_to_gcp(bucket, 'targeted_assertions.tar.gz', 'kgx/UniProt/targeted_assertions.tar.gz')


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(module)s:%(funcName)s:%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('Starting Main')
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--target', help='the export target: edges, nodes, or metadata')
    parser.add_argument('-i', '--instance', help='GCP DB instance name')
    parser.add_argument('-d', '--database', help='database name')
    parser.add_argument('-u', '--user', help='database username')
    parser.add_argument('-p', '--password', help='database password')
    parser.add_argument('-uni', '--uniprot_bucket', help='storage bucket for UniProt data')
    parser.add_argument('-c', '--chunk_size', help='number of assertions to process at a time', default=100, type=int)
    parser.add_argument('-l', '--limit', help='maximum number of publications to export per edge', default=5, type=int)
    parser.add_argument('-ao', '--assertion_offset', help='number of assertions to skip past', default=0, type=int)
    parser.add_argument('-al', '--assertion_limit', help='number of assertions to output', default=10000, type=int)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-e', '--kge_only', action='store_true')
    args = parser.parse_args()

    uniprot_bucket = args.uniprot_bucket if args.uniprot_bucket else 'test_kgx_output_bucket'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'prod-creds.json'

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.target == 'metadata':
        export_metadata(uniprot_bucket)
    else:
        models.init_db(
            instance=args.instance if args.instance else os.getenv('MYSQL_DATABASE_INSTANCE', None),
            user=args.user if args.user else os.getenv('MYSQL_DATABASE_USER', None),
            password=args.password if args.password else os.getenv('MYSQL_DATABASE_PASSWORD', None),
            database=args.database if args.database else 'text_mined_assertions'
        )
        session = models.session()

        logging.info("Exporting Targeted Assertion knowledge graph")
        logging.info("Exporting UniProt")
        if args.target == 'nodes':
            targeted.export_nodes(session, uniprot_bucket, 'kgx/UniProt/')
        else:
            targeted.export_kg(session, uniprot_bucket, 'kgx/UniProt/',
                               assertion_start=args.assertion_offset, assertion_limit=args.assertion_limit,
                               chunk_size=args.chunk_size, edge_limit=args.limit)
    logging.info("End Main")
