import gzip
import logging
import math
import shutil
import json
import os
from typing import Iterator

import models
import services
import sqlalchemy

ROW_BATCH_SIZE = 500000
HUMAN_TAXON = 'NCBITaxon:9606'
output_dir = 'out_c'
nodes_filename = os.path.join(output_dir, 'nodes.tsv.gz')
edges_filename = os.path.join(output_dir, 'edges.tsv.gz')
pairs_filename = os.path.join(output_dir, 'pairs.txt')
node_metadata = {}
edge_metadata = {}
sri_normalized_nodes = {}


def update_node_metadata(node: list[str]) -> None:
    category = node[2]
    prefix = node[0].split(':')[0]
    if category in node_metadata:
        if prefix not in node_metadata[category]["id_prefixes"]:
            node_metadata[category]["id_prefixes"].append(prefix)
        node_metadata[category]["count"] += 1
        node_metadata[category]["count_by_source"]["original_knowledge_source"]["infores:text-mining-provider-cooccurrence"] += 1
    else:
        node_metadata[category] = {
            "id_prefixes": [prefix],
            "count": 1,
            "count_by_source": {
                "original_knowledge_source": {
                    "infores:text-mining-provider-cooccurrence": 1
                }
            }
        }


def update_edge_metadata(edge: list) -> None:
    object_category = get_category(edge[0], normalized_nodes=sri_normalized_nodes)
    subject_category = get_category(edge[2], normalized_nodes=sri_normalized_nodes)
    triple = f"{object_category}|{edge[1]}|{subject_category}"
    relation = edge[4]
    if triple in edge_metadata:
        if relation not in edge_metadata[triple]["relations"]:
            edge_metadata[triple]["relations"].append(relation)
        edge_metadata[triple]["count"] += 1
        edge_metadata[triple]["count_by_source"]["original_knowledge_source"]["infores:text-mining-provider-cooccurrence"] += 1
    else:
        edge_metadata[triple] = {
            "subject": subject_category,
            "predicate": edge[1],
            "object": object_category,
            "relations": [relation],
            "count": 1,
            "count_by_source": {
                "original_knowledge_source": {
                    "infores:text-mining-provider-cooccurrence": 1
                }
            }
        }


def get_category(curie, normalized_nodes):
    category = 'biolink:SmallMolecule' if curie.startswith('DRUGBANK') else 'biolink:NamedThing'
    if curie in normalized_nodes and normalized_nodes[curie] is not None:
        category = normalized_nodes[curie]["type"][0]
    return category


def is_normal(curie: str, normalized_nodes: dict[str, dict]) -> bool:
    return curie in normalized_nodes and normalized_nodes[curie] is not None and \
           'id' in normalized_nodes[curie] and 'label' in normalized_nodes[curie]['id']


def get_kgx_nodes(curies: list[str], normalized_nodes:dict[str, dict]) -> Iterator[list[str]]:
    """
    Get the KGX node representation of a curie

    :param curies: the list of curies to turn into KGX nodes
    :param normalized_nodes: a dictionary of normalized nodes, for retrieving canonical label and category
    """
    for curie in curies:
        category = 'biolink:SmallMolecule' if curie.startswith('DRUGBANK') else 'biolink:NamedThing'
        if is_normal(curie, normalized_nodes):
            name = normalized_nodes[curie]['id']['label']
            if 'type' in normalized_nodes[curie]:
                category = normalized_nodes[curie]['type'][0]
            yield [curie, name, category]
        yield []


def write_nodes(session: sqlalchemy.orm.Session, outfile: str, use_uniprot: bool=False) -> None:
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
    global sri_normalized_nodes
    if use_uniprot:
        curies = [curie for curie in curies if not curie.startswith('PR:')]
    if len(curies) > 10000:
        sri_normalized_nodes = services.get_normalized_nodes_by_parts(curies, sublist_size=5000)
    else:
        sri_normalized_nodes = services.get_normalized_nodes(curies)
    with gzip.open(outfile, 'wb') as output:
        for node in get_kgx_nodes(curies, sri_normalized_nodes):
            if len(node) == 0:
                continue
            line = '\t'.join(node) + '\n'
            output.write(line.encode('utf-8'))
            update_node_metadata(node)
    logging.info('Node output complete')


def write_edges(session: sqlalchemy.orm.Session, filename: str, use_uniprot: bool=False) -> None:
    logging.info('Starting edge output')
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    # count_query_string = sqlalchemy.select(sqlalchemy.text('COUNT(1) FROM cooccurrence c LEFT JOIN cooccurrence_scores cs ON cs.cooccurrence_id = c.cooccurrence_id'))
    # record_count, = session.execute(count_query_string)
    # logging.info(f'Total edge records to export: {record_count}')
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
    with gzip.open(filename, 'wb') as outfile:
        for row in session.execute(data_query_string).yield_per(ROW_BATCH_SIZE):
            x += 1
            if x % (ROW_BATCH_SIZE / 4) == 0:
                logging.info(f"up to {x} rows")
            edge = get_edge_kgx(row, use_uniprot=use_uniprot)
            if len(edge) == 0:
                continue
            if not (is_normal(edge[0], sri_normalized_nodes) and is_normal(edge[2], sri_normalized_nodes)):
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
            update_edge_metadata(edge)
    with open(pairs_filename, 'w') as outfile:
        for pair in pair_set:
            outfile.write(pair + '\n')
    logging.info('Edge output complete')



def get_edge_kgx(row: list, use_uniprot=False) -> list:
    if use_uniprot:
        if row['curie1'].startswith('PR:') or row['curie2'].startswith('PR:'):
            return []
        if row['taxon1'] and row['taxon1'] != HUMAN_TAXON:
            return []
        if row['taxon2'] and row['taxon2'] != HUMAN_TAXON:
            return []
    return [row['curie1'], 'biolink:related_to', row['curie2'], row['cooccurrence_id'],
            'biolink:Association', f"tmkp:{row['cooccurrence_id']}_{row['level']}", json.dumps(get_json_attributes(row))]


def get_json_attributes(row: list):
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
        score_to_json(row['cooccurrence_id'], row['level'], row['concept1_count'], row['concept2_count'],
                      row['pair_count'], row['docstring'], float(row['ngd']), float(row['pmi']), float(row['pmi_norm']),
                      float(row['mutual_dependence']), float(row['pmi_norm_max']), float(row['lfmd']))
    ]
    return attributes_list


def score_to_json(id, level, c1_count, c2_count, pair_count, docs, ngd, pmi, pmi_norm, mutual_dependence, pmi_norm_max, lfmd):
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
                "description": f"The number of times concept #1 was observed to occur at the {level} level in the documents that were processed"
            },
            {
                "attribute_type_id": "biolink:tmkp_concept2_count",
                "value": c2_count,
                "value_type_id": "SIO:000794",
                "description": f"The number of times concept #2 was observed to occur at the {level} level in the documents that were processed"
            },
            {
                "attribute_type_id": "biolink:tmkp_concept_pair_count",
                "value": pair_count,
                "value_type_id": "SIO:000794",
                "description": f"The number of times the concepts of this assertion were observed to cooccur at the {level} level in the documents that were processed"
            },
            {
                "attribute_type_id": "biolink:tmkp_normalized_google_distance",
                "value": ngd,
                "value_type_id": "EDAM:data_1772",
                "description": f"The number of times the concepts of this assertion were observed to cooccur at the {level} level in the documents that were processed",
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
    logging.info(f"Total Cooccurrence Records: {cooccurrence_count}")
    logging.info(f"Total Partition Count: {partition_count}")
    pair_set = set()
    cooccurrence_query = sqlalchemy.select(models.Cooccurrence).execution_options(stream_results=True)
    with gzip.open(output_filename, "wb") as outfile:
        for partition_number in range(0, partition_count):
            for cooccurrence, in session.execute(cooccurrence_query.offset(partition_number * ROW_BATCH_SIZE).limit(ROW_BATCH_SIZE)):
                edge = cooccurrence.get_edge_kgx(use_uniprot)
                if len(edge) == 0:
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
                update_edge_metadata(edge)
            outfile.flush()
            logging.info(f"Done with partition {partition_number}")
    with open(pairs_filename, 'w') as outfile:
        for pair in pair_set:
            outfile.write(pair + '\n')
    logging.info('Edge output complete')


def create_kge_tarball() -> None:
    if not os.path.isdir('tmpc'):
        os.mkdir('tmpc')
    metadata_file = "tmpc/content_metadata.json"
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


def export_kg(session: sqlalchemy.orm.Session, bucket: str, blob_prefix: str, use_uniprot: bool=False) -> None:
    """
    Create and upload the node and edge KGX files for targeted assertions.

    :param session: the database session
    :param bucket: the output GCP bucket name
    :param blob_prefix: the directory prefix for the uploaded files
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    """
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    write_nodes(session, nodes_filename, use_uniprot=use_uniprot)
    services.upload_to_gcp(bucket, nodes_filename, f'{blob_prefix}cooccurrence_nodes.tsv.gz')
    write_edges(session, edges_filename, use_uniprot=use_uniprot)
    services.upload_to_gcp(bucket, edges_filename, f'{blob_prefix}cooccurrence_edges.tsv.gz')
    services.upload_to_gcp(bucket, pairs_filename, f'{blob_prefix}cooccurrence_pairs.txt')
    create_kge_tarball()
    services.upload_to_gcp(bucket, 'cooccurrence.tar.gz', f"{blob_prefix}cooccurrence.tar.gz")
