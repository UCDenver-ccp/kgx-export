import gzip
import logging
import os

import argparse
import targeted
import services

import pymysql.connections
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

GCP_BLOB_PREFIX = 'kgx/UniProt/'

def export_metadata(bucket):
    """
    Generate a metadata file from previously created KGX export files

    :param bucket: the GCP storage bucket containing the KGX files
    """
    services.get_from_gcp(bucket, GCP_BLOB_PREFIX + 'edges.tsv.gz', 'edges.tsv.gz')
    services.get_from_gcp(bucket, GCP_BLOB_PREFIX + 'nodes.tsv.gz', 'nodes.tsv.gz')
    services.generate_metadata('edges.tsv.gz', 'nodes.tsv.gz', 'KGE')
    services.upload_to_gcp(bucket, 'KGE/content_metadata.json', GCP_BLOB_PREFIX + 'content_metadata.json')


def get_valid_nodes(bucket) -> set[str]:
    """
    Retrieve the set of nodes used by a KGX nodes file

    :param bucket: the GCP storage bucket containing the KGX file
    :returns a set of node curies
    """
    services.get_from_gcp(bucket, GCP_BLOB_PREFIX + 'nodes.tsv.gz', 'nodes.tsv.gz')
    node_set = set([])
    with gzip.open('nodes.tsv.gz', 'rb') as infile:
        for line in infile:
            node_set.add(line.split(b'\t')[0].decode('utf-8'))
    return node_set


def init_db(instance: str, user: str, password: str, database: str) -> sessionmaker:  # pragma: no cover
    connector = Connector()

    def get_conn() -> pymysql.connections.Connection:
        conn: pymysql.connections.Connection = connector.connect(
            instance_connection_string=instance,
            driver='pymysql',
            user=user,
            password=password,
            database=database
        )
        return conn

    engine = create_engine('mysql+pymysql://', creator=get_conn, echo=False)
    maker = sessionmaker()
    maker.configure(bind=engine)
    return maker


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(module)s:%(funcName)s:%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('Starting Main')
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--target', help='the export target: edges, nodes, or metadata', required=True)
    parser.add_argument('-b', '--bucket', help='storage bucket for data', required=True)
    parser.add_argument('-i', '--instance', help='GCP DB instance name')
    parser.add_argument('-d', '--database', help='database name')
    parser.add_argument('-u', '--user', help='database username')
    parser.add_argument('-p', '--password', help='database password')
    parser.add_argument('-c', '--chunk_size', help='number of assertions to process at a time', default=100, type=int)
    parser.add_argument('-l', '--limit', help='maximum number of publications to export per edge', default=5, type=int)
    parser.add_argument('-ao', '--assertion_offset', help='number of assertions to skip past', default=0, type=int)
    parser.add_argument('-al', '--assertion_limit', help='number of assertions to output', default=10000, type=int)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    bucket = args.bucket if args.bucket else 'test_kgx_output_bucket'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'prod-creds.json'

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.target == 'metadata': # if we are just exporting metadata a database connection is not necessary
        export_metadata(bucket)
    else:
        session_maker = init_db(
            instance=args.instance if args.instance else os.getenv('MYSQL_DATABASE_INSTANCE', None),
            user=args.user if args.user else os.getenv('MYSQL_DATABASE_USER', None),
            password=args.password if args.password else os.getenv('MYSQL_DATABASE_PASSWORD', None),
            database=args.database if args.database else 'text_mined_assertions'
        )

        logging.info("Exporting Targeted Assertion knowledge graph")
        logging.info("Exporting UniProt")
        if args.target == 'nodes':
            targeted.export_nodes(session_maker(), bucket, GCP_BLOB_PREFIX)
        else:
            nodes = get_valid_nodes(bucket)
            targeted.export_edges(session_maker(), nodes, bucket, GCP_BLOB_PREFIX,
                                  assertion_start=args.assertion_offset, assertion_limit=args.assertion_limit,
                                  chunk_size=args.chunk_size, edge_limit=args.limit)
    logging.info("End Main")
