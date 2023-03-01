import gzip
import json
import logging
import math
import os
import shutil
from typing import Union, Any

import sqlalchemy
from sqlalchemy import text
from sqlalchemy.orm import Session

import models
import services

ROW_BATCH_SIZE = 10000
HUMAN_TAXON = 'NCBITaxon:9606'
ORIGINAL_KNOWLEDGE_SOURCE = "infores:text-mining-provider-targeted"
EXCLUDED_FIG_CURIES = ['DRUGBANK:DB10633', 'PR:000006421', 'PR:000008147', 'PR:000009005', 'PR:000031137',
                       'PR:Q04746', 'PR:Q04746', 'PR:Q7XZU3']


def get_node_data(session: Session, use_uniprot: bool = False) -> (list[str], dict[str, dict]):
    """
    Get the subject and object curies from assertions, uniquifies the list,
    and calls the SRI Node Normalizer service to get the dictionary.

    :param session: the database session.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :returns a tuple containing the list of unique curies and the normalization dictionary.
    """
    logging.info("Getting node data")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    if use_uniprot:
        curies = [row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, subject_curie) as curie '
                                                                  'FROM assertion LEFT JOIN pr_to_uniprot ON '
                                                                  'subject_curie = pr AND taxon = '
                                                                  '"NCBITaxon:9606"')).all()]
        curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, object_curie) as '
                                                                       'curie FROM assertion LEFT JOIN pr_to_uniprot '
                                                                       'ON object_curie = pr AND taxon = '
                                                                       '"NCBITaxon:9606"')).all()])
    else:
        curies = [row[0] for row in session.query(sqlalchemy.text('DISTINCT subject_curie FROM assertion')).all()]
        curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT object_curie FROM assertion')).all()])
    curies = list(set(curies))
    logging.info(f'node curies retrieved and uniquified ({len(curies)})')
    curies = [curie for curie in curies if curie not in EXCLUDED_FIG_CURIES]
    if use_uniprot:
        curies = [curie for curie in curies if not curie.startswith('PR:')]
    normalized_nodes = services.get_normalized_nodes(curies)
    return curies, normalized_nodes


def write_nodes(curies: list[str], normalize_dict: dict[str, dict], output_filename: str) -> dict:
    """
    Output the node data to a gzipped TSV file according to KGX node format.

    :param curies: the list of node curies.
    :param normalize_dict: the dictionary containing normalization information for the node curies.
    :param output_filename: filepath for the output file.
    :returns a metadata dictionary for the nodes that were written to file.
    """
    logging.info("Starting node output")
    metadata_dict = {}
    with gzip.open(output_filename, 'wb') as outfile:
        for node in services.get_kgx_nodes(curies, normalize_dict):
            if len(node) == 0:
                continue
            line = '\t'.join(node) + '\n'
            outfile.write(line.encode('utf-8'))
            metadata_dict = services.update_node_metadata(node, metadata_dict, ORIGINAL_KNOWLEDGE_SOURCE)
    logging.info('Node output complete')
    return metadata_dict


def get_assertion_ids(session, limit=600000, offset=0):
    id_query = text('SELECT assertion_id FROM assertion WHERE assertion_id NOT IN '
                    '(SELECT DISTINCT(assertion_id) '
                    'FROM evaluation INNER JOIN evidence '
                    'ON evidence.evidence_id = evaluation.evidence_id '
                    'WHERE overall_correct = 0 OR subject_correct = 0 '
                    'OR object_correct = 0 OR predicate_correct = 0) '
                    'AND subject_curie NOT IN :ex1 AND object_curie NOT IN :ex2 '
                    'ORDER BY assertion_id '
                    'LIMIT :limit OFFSET :offset'
                    )
    return [row[0] for row in session.execute(id_query, {
        'ex1': EXCLUDED_FIG_CURIES,
        'ex2': EXCLUDED_FIG_CURIES,
        'limit': limit,
        'offset': offset
    })]


def get_edge_data(session: Session, id_list, chunk_size=1000, edge_limit=5) -> list[str]:
    logging.info(f'\nStarting edge data gathering\nChunk Size: {chunk_size}\nEdge Limit: {edge_limit}\n')
    logging.info(f'Total Assertions: {len(id_list)}.')
    logging.info(f'Partition count: {math.ceil(len(id_list) / chunk_size)}')
    main_query = text(
        'SELECT a.assertion_id, e.evidence_id, a.association_curie, e.predicate_curie, '
        'a.subject_curie, su.uniprot AS subject_uniprot, a.object_curie, ou.uniprot AS object_uniprot, '
        'si.idf AS subject_idf, oi.idf AS object_idf, '
        'e.document_id, e.document_zone, e.document_year, e.score, '
        'e.sentence, e.subject_span, e.subject_text, e.object_span, e.object_text, '
        '(SELECT COUNT(1) FROM top_evidences t2 '
        'WHERE t2.assertion_id = a.assertion_id AND t2.predicate_curie = e.predicate_curie) AS evidence_count, '
        'IF(e.tm_id IS NULL, 0, 1) AS semmed_flag '
        'FROM assertion a '
        'INNER JOIN LATERAL '
        '(SELECT * FROM top_evidences te LEFT JOIN tm_semmed ts ON ts.tm_id = te.evidence_id '
        f'WHERE te.assertion_id = a.assertion_id ORDER BY ts.semmed_id IS NULL LIMIT {edge_limit}) AS e '
        'ON a.assertion_id = e.assertion_id '
        'LEFT JOIN pr_to_uniprot su ON a.subject_curie = su.pr AND su.taxon = "NCBITaxon:9606" '
        'LEFT JOIN pr_to_uniprot ou ON a.object_curie = ou.pr AND ou.taxon = "NCBITaxon:9606" '
        'LEFT JOIN concept_idf si ON a.subject_curie = si.concept_curie '
        'LEFT JOIN concept_idf oi ON a.object_curie = oi.concept_curie '
        'WHERE a.assertion_id IN :ids AND e.superseded_by IS NULL '
        'ORDER BY a.assertion_id'
    )
    # actual_list = []
    for i in range(0, len(id_list), chunk_size):
        slice_end = i + chunk_size if i + chunk_size < len(id_list) else len(id_list)
        logging.info(f'Working on slice [{i}:{slice_end}]')
        yield [row for row in session.execute(main_query, {'ids': id_list[i:slice_end]})]
        # actual_list.extend([row for row in session.execute(main_query, {'ids': id_list[i:slice_end]})])
    # logging.info(f'Retrieved {len(actual_list)} records')
    # return actual_list


# This is a simple transformation to group all evidence that belongs to the same assertion and make lookups possible.
def create_edge_dict(edge_data):
    edge_dict = {}
    for datum in edge_data:
        if datum['assertion_id'] not in edge_dict:
            edge_dict[datum['assertion_id']] = []
        edge_dict[datum['assertion_id']].append(datum)  # This is repetitive, but simpler. May need to change later.
    logging.debug(f'{len(edge_dict.keys())} distinct assertions')
    return edge_dict


def get_edge_metadata(edge_data, metadata_dict, normal_dict):
    # metadata_dict = {}
    for row in edge_data:
        metadata_dict = services.update_edge_metadata_2(row, metadata_dict, normal_dict, ORIGINAL_KNOWLEDGE_SOURCE)
    return metadata_dict


def write_edges_2(edge_dict, output_filename):
    logging.info("Starting edge output")
    skipped_assertions = set([])
    with gzip.open(output_filename, 'wb') as outfile:
        for assertion, rows in edge_dict.items():
            predicates = set([row['predicate_curie'] for row in rows])
            for predicate in predicates:
                edge = get_edge(rows, predicate)
                if not edge:
                    skipped_assertions.add(assertion)
                    continue
                line = '\t'.join(str(val) for val in edge) + '\n'
                throwaway_value = outfile.write(line.encode('utf-8'))
        outfile.flush()
    logging.info(f'{len(skipped_assertions)} distinct assertions were skipped')
    logging.info("Edge output complete")


def write_edges_3(edge_dict, output_filename):
    logging.info("Starting edge output")
    skipped_assertions = set([])
    with open(output_filename, 'a') as outfile:
        for assertion, rows in edge_dict.items():
            predicates = set([row['predicate_curie'] for row in rows])
            for predicate in predicates:
                edge = get_edge(rows, predicate)
                if not edge:
                    skipped_assertions.add(assertion)
                    continue
                line = '\t'.join(str(val) for val in edge) + '\n'
                throwaway_value = outfile.write(line)
        outfile.flush()
    logging.info(f'{len(skipped_assertions)} distinct assertions were skipped')
    logging.info("Edge output complete")


def get_edge(rows, predicate):
    relevant_rows = [row for row in rows if row['predicate_curie'] == predicate]
    if len(relevant_rows) == 0:
        logging.debug(f'No relevant rows for predicate {predicate}')
        return None
    row1 = relevant_rows[0]
    if (row1['object_curie'].startswith('PR:') and not row1['object_uniprot']) or \
            (row1['subject_curie'].startswith('PR:') and not row1['subject_uniprot']):
        logging.debug(f"Could not get uniprot for pr curie ({row1['object_curie']}|{row1['subject_curie']})")
        return None
    sub = row1['subject_uniprot'] if row1['subject_uniprot'] else row1['subject_curie']
    obj = row1['object_uniprot'] if row1['object_uniprot'] else row1['object_curie']
    supporting_study_results = '|'.join([f"tmkp:{row['evidence_id']}" for row in relevant_rows])
    supporting_publications = '|'.join([row['document_id'] for row in relevant_rows])
    qualified_predicate = ''
    subject_aspect_qualifier = ''
    subject_direction_qualifier = ''
    subject_part_qualifier = ''
    subject_form_or_variant_qualifier = ''
    object_aspect_qualifier = ''
    object_direction_qualifier = ''
    object_part_qualifier = ''
    object_form_or_variant_qualifier = ''
    anatomical_context_qualifier = ''
    if predicate == 'biolink:entity_positively_regulates_entity':
        predicate = 'biolink:affects'
        qualified_predicate = 'biolink:causes'
        object_aspect_qualifier = 'activity_or_abundance'
        object_direction_qualifier = 'increased'
    elif predicate == 'biolink:entity_negatively_regulates_entity':
        predicate = 'biolink:affects'
        qualified_predicate = 'biolink:causes'
        object_aspect_qualifier = 'activity_or_abundance'
        object_direction_qualifier = 'decreased'
    elif predicate == 'biolink:gain_of_function_contributes_to':
        predicate = 'biolink:affects'
        qualified_predicate = 'biolink:contributes_to'
        subject_form_or_variant_qualifier = 'gain_of_function_variant_form'
    elif predicate == 'biolink:loss_of_function_contributes_to':
        predicate = 'biolink:affects'
        qualified_predicate = 'biolink:contributes_to'
        subject_form_or_variant_qualifier = 'loss_of_function_variant_form'
    return [sub, predicate, obj, qualified_predicate,
            subject_aspect_qualifier, subject_direction_qualifier,
            subject_part_qualifier, subject_form_or_variant_qualifier,
            object_aspect_qualifier, object_direction_qualifier,
            object_part_qualifier, object_form_or_variant_qualifier,
            anatomical_context_qualifier,
            row1['assertion_id'], row1['association_curie'], get_aggregate_score(relevant_rows),
            supporting_study_results, supporting_publications, get_assertion_json(relevant_rows)]


def get_aggregate_score(rows):
    scores = []
    for row in rows:
        scores.append(get_score(row))
    return math.fsum(scores) / float(len(scores))


def get_score(row):
    base_score = float(row['score'])
    if not row['subject_idf'] or not row['object_idf']:
        return base_score
    else:
        return abs(math.log10(row['subject_idf']) * math.log10(row['object_idf']) * base_score)


def get_assertion_json(rows):
    semmed_count = sum([row['semmed_flag'] for row in rows])
    row1 = rows[0]
    attributes_list = [
        {
            "attribute_type_id": "biolink:original_knowledge_source",
            "value": "infores:text-mining-provider-targeted",
            "value_type_id": "biolink:InformationResource",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:supporting_data_source",
            "value": "infores:pubmed",
            "value_type_id": "biolink:InformationResource",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:has_evidence_count",
            "value": row1['evidence_count'],
            "value_type_id": "biolink:EvidenceCount",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:tmkp_confidence_score",
            "value": get_aggregate_score(rows),
            "value_type_id": "biolink:ConfidenceLevel",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:supporting_document",
            "value": '|'.join([row['document_id'] for row in rows]),
            "value_type_id": "biolink:Publication",
            "attribute_source": "infores:pubmed"
        }
    ]
    if semmed_count > 0:
        attributes_list.append({
            "attribute_type_id": "biolink:semmed_agreement_count",
            "value": semmed_count,
            "value_type_id": "SIO:000794",
            "attribute_source": "infores:text-mining-provider-targeted"
        })
    for row in rows:
        attributes_list.append(get_evidence_json(row))
    return json.dumps(attributes_list)


def get_evidence_json(row):
    nested_attributes = [
        {
            "attribute_type_id": "biolink:supporting_text",
            "value": row['sentence'],
            "value_type_id": "EDAM:data_3671",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:supporting_document",
            "value": row['document_id'],
            "value_type_id": "biolink:Publication",
            "value_url": f"https://pubmed.ncbi.nlm.nih.gov/{str(row['document_id']).split(':')[-1]}/",
            "attribute_source": "infores:pubmed"
        },
        {
            "attribute_type_id": "biolink:supporting_text_located_in",
            "value": row['document_zone'],
            "value_type_id": "IAO_0000314",
            "attribute_source": "infores:pubmed"
        },
        {
            "attribute_type_id": "biolink:extraction_confidence_score",
            "value": get_score(row),
            "value_type_id": "EDAM:data_1772",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:subject_location_in_text",
            "value": row['subject_span'] if row['subject_span'] else '',
            "value_type_id": "SIO:001056",
            "attribute_source": "infores:text-mining-provider-targeted"
        },
        {
            "attribute_type_id": "biolink:object_location_in_text",
            "value": row['object_span'] if row['object_span'] else '',
            "value_type_id": "SIO:001056",
            "attribute_source": "infores:text-mining-provider-targeted "
        }
    ]
    if row['document_year']:
        nested_attributes.append(
            {
                "attribute_type_id": "biolink:supporting_document_year",
                "value": row['document_year'],
                "value_type_id": "UO:0000036",
                "attribute_source": "infores:pubmed"
            }
        )
    if row['semmed_flag'] == 1:
        nested_attributes.append(
            {
                "attribute_type_id": "biolink:agrees_with_data_source",
                "value": "infores:semmeddb",
                "value_type_id": "biolink:InformationResource",
                "attribute_source": "infores:text-mining-provider-targeted"
            }
        )
    return {
        "attribute_type_id": "biolink:supporting_study_result",
        "value": f"tmkp:{row['evidence_id']}",
        "value_type_id": "biolink:TextMiningResult",
        "value_url": f"https://tmui.text-mining-kp.org/evidence/{row['evidence_id']}",
        "attribute_source": "infores:text-mining-provider-targeted",
        "attributes": nested_attributes
    }


def write_edges(session: Session, normalize_dict: dict[str, dict], output_filename: str, use_uniprot: bool = False, limit: int = 0) -> Union[dict[Any, Any], dict]:
    """
    Get the edge (or edges) associated with each assertion and output them to a gzipped TSV file according to KGX edge format.

    :param session: the database session.
    :param normalize_dict: the dictionary containing normalization information for checking the nodes associated with each edge.
    :param output_filename: filepath for the output file.
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :param limit: the maximum number of supporting study results per edge to include in the JSON blob (0 is no limit)
    :returns a metadata dictionary for the edges that were written to file.
    """
    logging.info("Starting edge output")
    logging.info(f"Mode: {'UniProt' if use_uniprot else 'PR'}")
    evaluation_subquery = session.query(sqlalchemy.text('DISTINCT(assertion_id) '
                                                        'FROM evaluation INNER JOIN evidence '
                                                        'ON evidence.evidence_id = evaluation.evidence_id '
                                                        'WHERE overall_correct = 0 OR subject_correct = 0 '
                                                        'OR object_correct = 0 OR predicate_correct = 0'))
    assertion_count = session.query(models.Assertion)\
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))\
        .count()
    partition_count = math.ceil(assertion_count / ROW_BATCH_SIZE)
    logging.info(f"Total Assertions: {assertion_count}")
    logging.info(f"Total Partition Count: {partition_count}")
    assertion_query = sqlalchemy.select(models.Assertion)\
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))\
        .execution_options(stream_results=True)
    metadata_dict = {}
    with gzip.open(output_filename, 'wb') as outfile:
        for partition_number in range(0, 5):
            for assertion, in session.execute(assertion_query.offset(partition_number * ROW_BATCH_SIZE).limit(ROW_BATCH_SIZE)):
                if use_uniprot:
                    edges = assertion.get_other_edges_kgx(limit)
                else:
                    edges = assertion.get_edges_kgx(limit)
                for edge in edges:
                    if len(edge) == 0:
                        continue
                    if not (services.is_normal(edge[0], normalize_dict) and services.is_normal(edge[2], normalize_dict)):
                        continue
                    line = '\t'.join(str(val) for val in edge) + '\n'
                    outfile.write(line.encode('utf-8'))
                    metadata_dict = services.update_edge_metadata(edge, metadata_dict, normalize_dict, ORIGINAL_KNOWLEDGE_SOURCE)
            outfile.flush()
            logging.info(f"Done with partition {partition_number}")
    logging.info("Edge output complete")
    return metadata_dict


def create_kge_tarball(directory: str, node_metadata: dict, edge_metadata: dict):
    logging.info("Starting KGE tarball creation")
    if not os.path.isdir(directory):
        os.mkdir(directory)
    node_file = os.path.join(directory, "nodes.tsv")
    edge_file = os.path.join(directory, "edges.tsv")
    metadata_file = os.path.join(directory, "content_metadata.json")

    metadata_dict = {
        "nodes": node_metadata,
        "edges": list(edge_metadata.values())
    }
    logging.info("Writing metadata file")
    with open(metadata_file, 'w') as outfile:
        outfile.write(json.dumps(metadata_dict))

    # We extract the files from gzip if they are not already in the temp directory
    if not (os.path.isfile(node_file) and os.path.isfile(edge_file)):
        logging.info("Could not find one or more of the text files. Attempting to extract them")
        with gzip.open('nodes.tsv.gz', 'rb') as file_in:
            with open(node_file, 'wb') as file_out:
                # Currently the gz files don't have headers, but metadata generation requires them.
                file_out.write('id\tname\tcategory\n'.encode('utf-8'))
                shutil.copyfileobj(file_in, file_out)
        headers = ['subject', 'predicate', 'object', 'qualified_predicate',
                   'subject_aspect_qualifier', 'subject_direction_qualifier',
                   'subject_part_qualifier', 'subject_form_or_variant_qualifier',
                   'object_aspect_qualifier', 'object_direction_qualifier',
                   'object_part_qualifier', 'object_form_or_variant_qualifier',
                   'anatomical_context_qualifier', 'id', 'relation', 'confidence_score',
                   'supporting_study_results', 'supporting_publications', '_attributes']
        with gzip.open('edges.tsv.gz', 'rb') as file_in:
            with open(edge_file, 'wb') as file_out:
                # Currently the gz files don't have headers, but metadata generation requires them.
                file_out.write('\t'.join(headers).encode('utf-8'))
                # file_out.write('subject\tpredicate\tobject\tid\trelation\tconfidence_score\tsupporting_study_results'
                #                '\tsupporting_publications\t_attributes\n'.encode('utf-8'))
                shutil.copyfileobj(file_in, file_out)
        logging.info("Extraction complete")

    logging.info("Creating tarball")
    shutil.make_archive('targeted_assertions', 'gztar', root_dir=directory)


def export_nodes(session: Session, bucket: str, blob_prefix: str):
    (node_curies, normal_dict) = get_node_data(session, use_uniprot=True)
    node_metadata = write_nodes(node_curies, normal_dict, 'nodes.tsv.gz')
    services.upload_to_gcp(bucket, 'nodes.tsv.gz', f'{blob_prefix}nodes.tsv.gz')


def export_kg(session: Session, bucket: str, blob_prefix: str,
              assertion_start: int = 0, assertion_limit: int = 600000,
              use_uniprot: bool = False, chunk_size=100, edge_limit: int = 0) -> None:  # pragma: no cover
    """
    Create and upload the node and edge KGX files for targeted assertions.

    :param session: the database session
    :param bucket: the output GCP bucket name
    :param blob_prefix: the directory prefix for the uploaded files
    :param use_uniprot: whether to translate the PR curies to UniProt (curies with no UniProt equivalent will be excluded)
    :param chunk_size: the number of assertions to process at a time
    :param edge_limit: the maximum number of supporting study results per edge to include in the JSON blob (0 is no limit)
    """
    output_filename = f'edges_{assertion_start}_{assertion_start + assertion_limit}.tsv'
    id_list = get_assertion_ids(session, limit=assertion_limit, offset=assertion_start)
    # edge_metadata = {}
    for rows in get_edge_data(session, id_list, chunk_size, edge_limit):
        logging.info(f'Processing the next {len(rows)} rows')
        edge_dict = create_edge_dict(rows)
        # edge_metadata = get_edge_metadata(rows, edge_metadata, normal_dict)
        write_edges_3(edge_dict, output_filename)
    services.upload_to_gcp(bucket, output_filename, f'{blob_prefix}{output_filename}')
    # with open('edges.tsv', 'rb') as edgefile:
    #     with gzip.open('edges.tsv.gz', 'wb') as gzfile:
    #         shutil.copyfileobj(edgefile, gzfile)
    # edge_data = get_edge_data(session, id_list, chunk_size, edge_limit)
    # edge_metadata = get_edge_metadata(edge_data, normal_dict)
    # edge_dict = create_edge_dict(edge_data)
    # write_edges_2(edge_dict, 'edges.tsv.gz')
    # edge_metadata = write_edges(session, normal_dict, "edges.tsv.gz", use_uniprot=use_uniprot, limit=edge_limit)

    # services.upload_to_gcp(bucket, 'edges.tsv.gz', f'{blob_prefix}edges.tsv.gz')
    # create_kge_tarball('tmp', node_metadata, edge_metadata)
    # os.remove('edges.tsv')
    # services.upload_to_gcp(bucket, 'targeted_assertions.tar.gz', f'{blob_prefix}targeted_assertions.tar.gz')
