import sqlalchemy
import os
import models

def get_nodes():
    subject_nodes = object_nodes = None
    node_sql = """
    SELECT assertion.subject_curie AS node_curie, covered_text, "biolink:SubjectCategory" as category
    FROM assertion 
        LEFT JOIN evidence ON (assertion.assertion_id = evidence.assertion_id)
        LEFT JOIN entity ON (evidence.subject_entity_id = entity.entity_id)
    UNION 
    SELECT assertion.object_curie AS node_curie, covered_text, "biolink:ObjectCategory"
    FROM assertion 
        LEFT JOIN evidence ON (assertion.assertion_id = evidence.assertion_id)
        LEFT JOIN entity ON (evidence.object_entity_id = entity.entity_id);"""
    with engine.connect() as connection:
        node_results = connection.execute(sqlalchemy.text(node_sql))
    return [(row.node_curie, row.covered_text, row.category) for row in node_results]

def get_edges():
    predicate_sql = """
    SELECT subject_curie, predicate_curie, object_curie, assertion.assertion_id, association_curie, 
        '??' as confidence_score, '???' as supporting_study_results, document_id
    FROM assertion 
        LEFT JOIN evidence ON (assertion.assertion_id = evidence.assertion_id)
        LEFT JOIN evidence_score ON (evidence.evidence_id = evidence_score.evidence_id)"""
    with engine.connect() as connection:
        return [(row.subject_curie, row.predicate_curie, row.object_curie, row.assertion_id, row.association_curie,
                 row.confidence_score, row.supporting_study_results, row.document_id)
                for row in connection.execute(sqlalchemy.text(predicate_sql))]


def get_assertions():
    s = models.session()
    assertions = s.query(models.Assertion).all()
    print(assertions[0].get_json_attributes())


if __name__ == "__main__":
    get_assertions()
