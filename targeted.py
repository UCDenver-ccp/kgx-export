import gzip
import logging
import math

import sqlalchemy
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import declarative_base

import services
Model = declarative_base(name='Model')

ROW_BATCH_SIZE = 10000
HUMAN_TAXON = 'NCBITaxon:9606'
ORIGINAL_KNOWLEDGE_SOURCE = "infores:text-mining-provider-targeted"
EXCLUDED_FIG_CURIES = ['DRUGBANK:DB10633', 'PR:000006421', 'PR:000008147', 'PR:000009005', 'PR:000031137',
                       'PR:Q04746', 'PR:Q04746', 'PR:Q7XZU3']


class Evidence(Model):
    __tablename__ = 'evidence'
    evidence_id = Column(String(65), primary_key=True)
    assertion_id = Column(String(65))
    document_id = Column(String(45))
    sentence = Column(String(2000))
    subject_entity_id = Column(String(65))
    object_entity_id = Column(String(65))
    document_zone = Column(String(45))
    document_publication_type = Column(String(100))
    document_year_published = Column(Integer)
    superseded_by = Column(String(20))

    def __init__(self, evidence_id, assertion_id, document_id, sentence, subject_entity_id, object_entity_id,
                 document_zone, document_publication_type, document_year_published, superseded_by):
        self.evidence_id = evidence_id
        self.assertion_id = assertion_id
        self.document_id = document_id
        self.sentence = sentence
        self.subject_entity_id = subject_entity_id
        self.object_entity_id = object_entity_id
        self.document_zone = document_zone
        self.document_publication_type = document_publication_type
        self.document_year_published = document_year_published
        self.superseded_by = superseded_by


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
                                                                  'subject_curie = pr AND '
                                                                  f'taxon = "{HUMAN_TAXON}"')).all()]
        curies.extend([row[0] for row in session.query(sqlalchemy.text('DISTINCT IFNULL(uniprot, object_curie) as '
                                                                       'curie FROM assertion LEFT JOIN pr_to_uniprot '
                                                                       f'ON object_curie = pr AND '
                                                                       f'taxon = "{HUMAN_TAXON}"')).all()])
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
                    'FROM assertion_evidence_feedback af '
                    'INNER JOIN evidence_feedback_answer ef '
                    'INNER JOIN evidence e ON e.evidence_id = af.evidence_id '
                    'INNER JOIN evidence_version ev ON ev.evidence_id = e.evidence_id '
                    'WHERE ef.prompt_text = \'Assertion Correct\' AND ef.response = 0 AND ev.version = 2) '
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
        '(SELECT COUNT(1) FROM top_unique_evidences t2 '
        'WHERE t2.assertion_id = a.assertion_id AND t2.predicate_curie = e.predicate_curie) AS evidence_count, '
        'IF(e.tm_id IS NULL, 0, 1) AS semmed_flag '
        'FROM assertion a '
        'INNER JOIN LATERAL '
        '(SELECT * FROM top_unique_evidences te LEFT JOIN tm_semmed ts ON ts.tm_id = te.evidence_id '
        f'WHERE te.assertion_id = a.assertion_id ORDER BY ts.semmed_id IS NULL LIMIT {edge_limit}) AS e '
        'ON a.assertion_id = e.assertion_id '
        f'LEFT JOIN pr_to_uniprot su ON a.subject_curie = su.pr AND su.taxon = "{HUMAN_TAXON}" '
        f'LEFT JOIN pr_to_uniprot ou ON a.object_curie = ou.pr AND ou.taxon = "{HUMAN_TAXON}" '
        'LEFT JOIN concept_idf si ON a.subject_curie = si.concept_curie '
        'LEFT JOIN concept_idf oi ON a.object_curie = oi.concept_curie '
        'WHERE a.assertion_id IN :ids AND e.document_zone <> "REF" AND e.superseded_by IS NULL '
        'ORDER BY a.assertion_id'
    )
    for i in range(0, len(id_list), chunk_size):
        slice_end = i + chunk_size if i + chunk_size < len(id_list) else len(id_list)
        logging.info(f'Working on slice [{i}:{slice_end}]')
        yield [row for row in session.execute(main_query, {'ids': id_list[i:slice_end]})]


def get_superseded_chunk(session: Session) -> list[tuple[str, str]]:
    logging.info("get_superseded_chunk")
    query_text = text("""
    SELECT e1.evidence_id, e2.document_id
    FROM assertion a1 
        INNER JOIN evidence e1 ON (e1.assertion_id = a1.assertion_id) 
        INNER JOIN top_evidence_scores es1 ON (es1.evidence_id = e1.evidence_id)
        INNER JOIN pubmed_to_pmc t ON t.pmid = e1.document_id
        INNER JOIN evidence e2 ON (e2.document_id = t.pmcid)
        INNER JOIN top_evidence_scores es2 ON (es2.evidence_id = e2.evidence_id)
        INNER JOIN assertion a2 ON (a2.assertion_id = e2.assertion_id)
    WHERE 
        e1.document_id LIKE 'PMID%' 
        AND e1.superseded_by IS NULL
        AND e1.document_id IN (SELECT pmid FROM pubmed_to_pmc)
        AND e1.sentence = e2.sentence
        AND e1.document_zone = e2.document_zone
        AND a1.subject_curie = a2.subject_curie
        AND a1.object_curie = a2.object_curie
        AND es1.predicate_curie = es2.predicate_curie
    LIMIT 10000
    """)
    eids = set([])
    ids_list = []
    for row in session.execute(query_text):
        eid = row['evidence_id']
        did = row['document_id']
        if eid not in eids:
            ids_list.append((eid, did))
            eids.add(eid)
    logging.info(len(ids_list))
    return ids_list


def update_superseded_by(session: Session, ids_list: list[tuple[str, str]]) -> None:
    logging.info("starting update function")
    mappings = []
    for ids in ids_list:
        mappings.append({
            'evidence_id': ids[0],
            'superseded_by': ids[1]
        })
    # print(mappings)
    logging.info('about to update: ' + str(len(mappings)))
    session.bulk_update_mappings(Evidence, mappings)
    logging.info('bulk update created')
    session.commit()
    logging.info('update committed')


# This is a simple transformation to group all evidence that belongs to the same assertion and make lookups possible.
def create_edge_dict(edge_data):
    edge_dict = {}
    for datum in edge_data:
        if datum['assertion_id'] not in edge_dict:
            edge_dict[datum['assertion_id']] = []
        edge_dict[datum['assertion_id']].append(datum)  # This is repetitive, but simpler. May need to change later.
    logging.debug(f'{len(edge_dict.keys())} distinct assertions')
    return edge_dict


def export_nodes(session: Session, bucket: str, blob_prefix: str):
    logging.info("Exporting Nodes")
    (node_curies, normal_dict) = get_node_data(session, use_uniprot=True)
    node_metadata = write_nodes(node_curies, normal_dict, 'nodes.tsv.gz')
    services.upload_to_gcp(bucket, 'nodes.tsv.gz', f'{blob_prefix}nodes.tsv.gz')


def export_edges(session: Session, nodes: set, bucket: str, blob_prefix: str,
                 assertion_start: int = 0, assertion_limit: int = 600000,
                 chunk_size=100, edge_limit: int = 5) -> None:  # pragma: no cover
    """
    Create and upload the node and edge KGX files for targeted assertions.

    :param session: the database session
    :param nodes: a set of curies that appear in the nodes KGX file
    :param bucket: the output GCP bucket name
    :param blob_prefix: the directory prefix for the uploaded files
    :param assertion_start: offset for assertion query
    :param assertion_limit: limit for assertion query
    :param chunk_size: the number of assertions to process at a time
    :param edge_limit: the maximum number of supporting study results per edge to include in the JSON blob (0 is no limit)
    """
    output_filename = f'edges_{assertion_start}_{assertion_start + assertion_limit}.tsv'
    id_list = get_assertion_ids(session, limit=assertion_limit, offset=assertion_start)
    for rows in get_edge_data(session, id_list, chunk_size, edge_limit):
        logging.info(f'Processing the next {len(rows)} rows')
        edge_dict = create_edge_dict(rows)
        services.write_edges(edge_dict, nodes, output_filename)
    services.upload_to_gcp(bucket, output_filename, f'{blob_prefix}{output_filename}')
