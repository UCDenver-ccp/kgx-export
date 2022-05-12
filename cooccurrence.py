import gzip
import logging
import math
import shutil
import json
import os
import csv
from typing import Iterator, NamedTuple

import models
import services
import sqlalchemy

ROW_BATCH_SIZE = 500000
HUMAN_TAXON = 'NCBITaxon:9606'
ORIGINAL_KNOWLEDGE_SOURCE = "infores:text-mining-provider-cooccurrence"
output_dir = 'out_c'
nodes_filename = os.path.join(output_dir, 'nodes.tsv.gz')
edges_filename = os.path.join(output_dir, 'edges.tsv.gz')
pairs_filename = os.path.join(output_dir, 'pairs.txt')
sri_normalized_nodes = {}


def generate_metadata(nodes_filename: str, edges_filename: str, compressed: bool=True) -> dict:
    """
    Creates a metadata dictionary for existing nodes and edges files in KGX format

    :param nodes_filename: the file path for the nodes KGX file
    :param edges_filename: the file path for the edges KGX file
    :returns the full metadata dictionary
    """
    nodes_metadata_dict = {}
    edges_metadata_dict = {}
    node_curies = []
    logging.info("Node metadata start")
    if compressed:
        nodes_file = gzip.open(nodes_filename, 'rt')
    else:
        nodes_file = open(nodes_filename, 'r')
    reader = csv.reader(nodes_file, delimiter='\t')
    for node in reader:
        node_curies.append(node[0])
        nodes_metadata_dict = services.update_node_metadata(node, nodes_metadata_dict, ORIGINAL_KNOWLEDGE_SOURCE)
    nodes_file.close()
    logging.info("Node metadata end")
    normalized_nodes = services.get_normalized_nodes(node_curies)
    logging.info("Edge metadata start")
    if compressed:
        edges_file = gzip.open(edges_filename, 'rt')
    else:
        edges_file = open(edges_filename, 'r')
    reader = csv.reader(edges_file, delimiter='\t')
    for edge in reader:
        edges_metadata_dict = services.update_edge_metadata(edge, edges_metadata_dict, normalized_nodes, ORIGINAL_KNOWLEDGE_SOURCE)
    edges_file.close()
    logging.info("Edge metadata end")
    return {
        "nodes": nodes_metadata_dict,
        "edges": list(edges_metadata_dict.values())
    }


def write_nodes(session: sqlalchemy.orm.Session, outfile: str, use_uniprot: bool=False) -> (dict, dict[str, dict]):
    """
    Get the subject and object curies from cooccurrences, normalize and uniquify them, and then output to a gzipped TSV file according to KGX node format.

    :param session: the database session.
    :param output_filename: filepath for the output file.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    """
    logging.info("Starting node export")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    curies = []
    if use_uniprot:
        entity1_curie_query = sqlalchemy.select(sqlalchemy.text(
            """
            IFNULL(uniprot, entity1_curie) as curie 
            FROM cooccurrence 
                LEFT JOIN pr_to_uniprot ON entity1_curie = pr AND taxon = "NCBITaxon:9606"
            """
        ))
    else:
        entity1_curie_query = sqlalchemy.select(sqlalchemy.text(
            """entity1_curie FROM cooccurrence"""
        ))

    for curie, in session.execute(entity1_curie_query.execution_options(stream_results=True)):
        curies.append(curie)
        if len(curies) % (ROW_BATCH_SIZE / 10) == 0:
            curies = list(set(curies))
    curies = list(set(curies))
    logging.info(f'Got unique entity1 curies ({len(curies)})')
    if use_uniprot:
        entity2_curie_query = sqlalchemy.select(sqlalchemy.text(
            """
            IFNULL(uniprot, entity2_curie) as curie 
            FROM cooccurrence 
            LEFT JOIN pr_to_uniprot ON entity2_curie = pr AND taxon = "NCBITaxon:9606"
            """
        ))
    else:
        entity2_curie_query = sqlalchemy.select(sqlalchemy.text(
            """entity2_curie FROM cooccurrence"""
        ))
    for curie, in session.execute(entity2_curie_query.execution_options(stream_results=True)):
        curies.append(curie)
        if len(curies) % (ROW_BATCH_SIZE / 10) == 0:
            curies = list(set(curies))
    curies = list(set(curies))
    logging.info(f'unique node curies retrieved and uniquified ({len(curies)})')
    normalized_nodes = {}
    if use_uniprot:
        curies = [curie for curie in curies if not curie.startswith('PR:')]
    if len(curies) > 10000:
        normalized_nodes = services.get_normalized_nodes_by_parts(curies, sublist_size=15000)
    else:
        normalized_nodes = services.get_normalized_nodes(curies)
    metadata_dict = {}
    with gzip.open(outfile, 'wb') as output:
        for node in services.get_kgx_nodes(curies, normalized_nodes):
            if len(node) == 0:
                continue
            line = '\t'.join(node) + '\n'
            output.write(line.encode('utf-8'))
            metadata_dict = services.update_node_metadata(node, metadata_dict, ORIGINAL_KNOWLEDGE_SOURCE)
    logging.info('Node output complete')
    return (metadata_dict, normalized_nodes)


def write_edges(session: sqlalchemy.orm.Session, normalize_dict: dict[str, dict], output_filename: str, use_uniprot: bool=False) -> dict:
    """
    Get the edge (or edges) associated with each cooccurrence record and output them to a gzipped TSV file according to KGX edge format.

    :param session: the database session.
    :param normalize_dict: the dictionary containing normalization information for checking the nodes associated with each edge.
    :param output_filename: file path for the output file.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :returns a metadata dictionary for the edges that were written to file.
    """
    logging.info('Starting edge output')
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    count_query_string = sqlalchemy.select(sqlalchemy.text('COUNT(1) FROM cooccurrence'))
    record_count, = session.execute(count_query_string)
    logging.info(f'Total cooccurrence records to export: {record_count} (note: final edge count will probably be different.')
    if use_uniprot:
        data_query_string = sqlalchemy.select(sqlalchemy.text(
            """
            IFNULL(u1.uniprot, entity1_curie) as curie1, u1.taxon as taxon1, 
            IFNULL(u2.uniprot, entity2_curie) as curie2, u2.taxon as taxon2, cs.*, 
            (
                SELECT GROUP_CONCAT(document_id SEPARATOR '|') 
                FROM cooccurrence_publication
                WHERE cooccurrence_id = c.cooccurrence_id AND level = cs.level
                GROUP BY cooccurrence_id, level
            ) as docstring
            FROM cooccurrence c 
            LEFT JOIN pr_to_uniprot u1 ON c.entity1_curie = u1.pr 
            LEFT JOIN pr_to_uniprot u2 ON c.entity2_curie = u2.pr
            LEFT JOIN cooccurrence_scores cs ON cs.cooccurrence_id = c.cooccurrence_id
            """
        )).execution_options(stream_results=True)
    else:
        data_query_string = sqlalchemy.select(sqlalchemy.text(
            """
            entity1_curie as curie1 
            entity2_curie as curie2, 
            cs.*, 
            (
                SELECT GROUP_CONCAT(document_id SEPARATOR '|') 
                FROM cooccurrence_publication
                WHERE cooccurrence_id = c.cooccurrence_id AND level = cs.level
                GROUP BY cooccurrence_id, level
            ) as docstring
            FROM cooccurrence c 
            LEFT JOIN cooccurrence_scores cs ON cs.cooccurrence_id = c.cooccurrence_id
            """
        )).execution_options(stream_results=True)
    x = 0
    pair_set = set()
    edge_metadata = {}
    with gzip.open(output_filename, 'wb') as outfile:
        for row in session.execute(data_query_string).yield_per(ROW_BATCH_SIZE):
            x += 1
            if x % (ROW_BATCH_SIZE / 4) == 0:
                logging.info(f"up to {x} rows")
            edge = get_edge_kgx(row, use_uniprot=use_uniprot)
            if len(edge) == 0:
                continue
            if not (services.is_normal(edge[0], normalize_dict) and services.is_normal(edge[2], normalize_dict)):
                continue
            c1 = edge[0].split(':')[0]
            c2 = edge[2].split(':')[0]
            if c1 < c2:
                pair = f"{c1}:{c2}"
            else:
                pair = f"{c2}:{c1}"
            pair_set.add(pair)
            line = '\t'.join(edge) + '\n'
            outfile.write(line.encode('utf-8'))
            edge_metadata = services.update_edge_metadata(edge, edge_metadata, normalize_dict, ORIGINAL_KNOWLEDGE_SOURCE)
    with open(pairs_filename, 'w') as outfile:
        for pair in pair_set:
            outfile.write(pair + '\n')
    logging.info('Edge output complete')
    return edge_metadata


def get_edge_kgx(row: dict, use_uniprot=False) -> list:
    """
    Formats a database row as a list in KGX format

    :param row: the database query result row
    :param use_uniprot: whether to skip edges containing PR curies or non-human taxons
    :returns a list containing the KGX format columns, or an empty list if the edge has been excluded by use_uniprot
    """
    if use_uniprot:
        if row['curie1'].startswith('PR:') or row['curie2'].startswith('PR:'):
            return []
        if row['taxon1'] and row['taxon1'] != HUMAN_TAXON:
            return []
        if row['taxon2'] and row['taxon2'] != HUMAN_TAXON:
            return []
    return [row['curie1'], 'biolink:related_to', row['curie2'], row['cooccurrence_id'],
            'biolink:Association', f"tmkp:{row['cooccurrence_id']}_{row['level']}", json.dumps(get_json_attributes(row))]


def get_json_attributes(row: dict) -> list:
    """
    Creates the JSON Blob for the edge KGX output

    :param row: the database query result row
    :returns a list conforming to the _attributes JSON Blob format
    """
    attributes_list = [
        {
            "attribute_type_id": "biolink:original_knowledge_source",
            "value": "infores:text-mining-provider-cooccurrence",
            "value_type_id": "biolink:InformationResource",
            "description": "The Text Mining Provider Concept Cooccurrence KP from NCATS Translator provides cooccurrence metrics for text-mined concepts that cooccur at various levels, e.g. document, sentence, etc. in the biomedical literature.",
            "attribute_source": "infores:text-mining-provider-cooccurrence"
        },
        {
            "attribute_type_id": "biolink:supporting_data_source",
            "value": "infores:pubmed",
            "value_type_id": "biolink:InformationResource",
            "attribute_source": "infores:text-mining-provider-cooccurrence"
        },
        scores_to_json(row['cooccurrence_id'], row['level'], row['concept1_count'], row['concept2_count'],
                       row['pair_count'], row['docstring'], float(row['ngd']), float(row['pmi']), float(row['pmi_norm']),
                       float(row['mutual_dependence']), float(row['pmi_norm_max']), float(row['lfmd']))
    ]
    return attributes_list


def scores_to_json(id: str, level: str, c1_count: int, c2_count: int, pair_count: int, docs: str,
                   ngd: float, pmi: float, pmi_norm: float, mutual_dependence: float, pmi_norm_max: float, lfmd: float):
    """
    Creates the JSON Blob attributes describing the various cooccurrence counts and scores
    """
    biolink_level = 'biolink:DocumentLevelConceptCooccurrenceAnalysisResult'
    desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur at the document level'
    if level == 'document':
        biolink_level = 'biolink:DocumentLevelConceptCooccurrenceAnalysisResult'
        desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur at the document level'
    elif level == 'sentence':
        biolink_level = 'biolink:SentenceLevelConceptCooccurrenceAnalysisResult'
        desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur at the sentence level'
    elif level == 'title':
        biolink_level = 'biolink:TitleLevelConceptCooccurrenceAnalysisResult'
        desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur in the document title'
    elif level == 'abstract':
        biolink_level = 'biolink:AbstractLevelConceptCooccurrenceAnalysisResult'
        desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur in the abstract'
    return {
        "attribute_type_id": "biolink:supporting_study_result",
        "value": f"tmkp:{id}_{level}",
        "value_type_id": biolink_level,
        "description": desc,
        "attribute_source": "infores:text-mining-provider-cooccurrence",
        "attributes": [
            {
                "attribute_type_id": "biolink:supporting_document",
                "value": docs,
                "value_type_id": "biolink:Publication",
                "description": f"The documents where the concepts of this assertion were observed to cooccur at the {level} level.",
                "attribute_source": "infores:pubmed"
            },
            {
                "attribute_type_id": "biolink:tmkp_concept1_count",
                "value": c1_count,
                "value_type_id": "SIO:000794",
                "description": f"The number of times concept #1 was observed to occur at the {level} level in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            },
            {
                "attribute_type_id": "biolink:tmkp_concept2_count",
                "value": c2_count,
                "value_type_id": "SIO:000794",
                "description": f"The number of times concept #2 was observed to occur at the {level} level in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            },
            {
                "attribute_type_id": "biolink:tmkp_concept_pair_count",
                "value": pair_count,
                "value_type_id": "SIO:000794",
                "description": f"The number of times the concepts of this assertion were observed to cooccur at the {level} level in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            },
            {
                "attribute_type_id": "biolink:tmkp_normalized_google_distance",
                "value": ngd,
                "value_type_id": "EDAM:data_1772",
                "description": "The normalized google distance score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            },
            {
                "attribute_type_id": "biolink:tmkp_pointwise_mutual_information",
                "value": pmi,
                "value_type_id": "EDAM:data_1772",
                "description": "The pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            },
            {
                "attribute_type_id": "biolink:tmkp_normalized_pointwise_mutual_information",
                "value": pmi_norm,
                "value_type_id": "EDAM:data_1772",
                "description": "The normalized pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            },
            {
                "attribute_type_id": "biolink:tmkp_mutual_dependence",
                "value": mutual_dependence,
                "value_type_id": "EDAM:data_1772",
                "description": "The mutual dependence (PMI^2) score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            },
            {
                "attribute_type_id": "biolink:tmkp_normalized_pointwise_mutual_information_max",
                "value": pmi_norm_max,
                "value_type_id": "EDAM:data_1772",
                "description": "A variant of the normalized pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            },
            {
                "attribute_type_id": "biolink:tmkp_log_frequency_biased_mutual_dependence",
                "value": lfmd,
                "value_type_id": "EDAM:data_1772",
                "description": "The log frequency biased mutual dependence score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                "attribute_source": "infores:text-mining-provider-cooccurrence"
            }
        ]
    }


def create_kge_tarball(bucket: str, blob_prefix: str, node_metadata: dict, edge_metadata:dict) -> None:
    """
    Repackages the node and edge files, together with a metadata file, into a single tarball for KGE

    :param bucket: the GCP bucket name, to download the files if they are not available locally
    :param blob_prefix: the directory prefix for the files on GCP
    :param node_metadata: the precalculated node metadata dictionary
    :param edge_metadata: the precalculated edge metadata dictionary
    """
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    if not os.path.isdir('tmpc'):
        os.mkdir('tmpc')
    if not os.path.isfile(nodes_filename):
        logging.debug("Downloading nodes file")
        services.get_from_gcp(bucket, f'{blob_prefix}cooccurrence_nodes.tsv.gz', nodes_filename)
    if not os.path.isfile(edges_filename):
        logging.debug("Downloading edges file")
        services.get_from_gcp(bucket, f'{blob_prefix}cooccurrence_edges.tsv.gz', edges_filename)
    metadata_file = "tmpc/content_metadata.json"
    if len(node_metadata) == 0 or len(edge_metadata) == 0:
        metadata_dict = generate_metadata(nodes_filename, edges_filename, True)
    else:
        metadata_dict = {
            "nodes": node_metadata,
            "edges": list(edge_metadata.values())
        }

    logging.info("Writing metadata file")
    with open(metadata_file, 'w') as outfile:
        outfile.write(json.dumps(metadata_dict))

    logging.info("Unpacking nodes file")
    with gzip.open(nodes_filename, 'rb') as file_in:
        with open('tmpc/nodes.tsv', 'wb') as file_out:
            file_out.write('id\tname\tcategory\n'.encode('utf-8'))
            shutil.copyfileobj(file_in, file_out)

    logging.info("Unpacking edges file")
    with gzip.open(edges_filename, 'rb') as file_in:
        with open('tmpc/edges.tsv', 'wb') as file_out:
            file_out.write('subject\tpredicate\tobject\tid\trelation\tsupporting_study_results\t_attributes\n'.encode('utf-8'))
            shutil.copyfileobj(file_in, file_out)

    logging.info("Creating tarball")
    shutil.make_archive('cooccurrence', 'gztar', root_dir='tmpc')


def export_kg(session: sqlalchemy.orm.Session, bucket: str, blob_prefix: str, use_uniprot: bool=False) -> None: # pragma: no cover
    """
    Create and upload the node and edge KGX files for targeted assertions.

    :param session: the database session
    :param bucket: the output GCP bucket name
    :param blob_prefix: the directory prefix for the uploaded files
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    """
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    (node_metadata, normal_dict) = write_nodes(session, nodes_filename, use_uniprot=use_uniprot)
    services.upload_to_gcp(bucket, nodes_filename, f'{blob_prefix}cooccurrence_nodes.tsv.gz')
    edge_metadata = write_edges(session, normal_dict, edges_filename, use_uniprot=use_uniprot)
    services.upload_to_gcp(bucket, edges_filename, f'{blob_prefix}cooccurrence_edges.tsv.gz')
    services.upload_to_gcp(bucket, pairs_filename, f'{blob_prefix}cooccurrence_pairs.txt')
    create_kge_tarball(bucket, blob_prefix, node_metadata, edge_metadata)
    services.upload_to_gcp(bucket, 'cooccurrence.tar.gz', f"{blob_prefix}cooccurrence.tar.gz")
