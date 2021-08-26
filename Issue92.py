import sqlalchemy
import os
import models
import services
from sqlalchemy.orm import joinedload

#TODO: add context managers to all uses of session()
def get_nodes():
    services.log_timestamp("Start GN")
    s = models.session()
    evaluation_subquery = s.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    assertions = s.query(models.Assertion).filter(models.Assertion.assertion_id.notin_(evaluation_subquery))
    services.log_timestamp("Got Assertions GN")
    curies = [assertion.object_curie for assertion in assertions]
    services.log_timestamp("Got Curies 1 GN")
    curies.extend([assertion.subject_curie for assertion in assertions])
    services.log_timestamp("Got Curies 2 GN")
    normalized_nodes = services.get_normalized_nodes(curies)
    node_list = []
    for assertion in assertions:
        node_list.extend(assertion.get_node_kgx(normalized_nodes))
    services.log_timestamp("Got node_list GN")
    unique_curies = set([])
    nodes = []
    for node in node_list:
        if node[0] not in unique_curies:
            unique_curies.add(node[0])
            nodes.append(node)
    services.log_timestamp("Got nodes GN")
    with open('C:\\Users\\edgar\\source\\repos\\Issue92\\nodes.tsv', 'w') as outfile:
        for node in nodes:
            outfile.write('\t'.join(str(val) for val in node))
            outfile.write('\n')
    services.log_timestamp("End GN")


def get_other_nodes():
    services.log_timestamp("Start GON")
    s = models.session()
    evaluation_subquery = s.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    assertions = s.query(models.Assertion).filter(models.Assertion.assertion_id.notin_(evaluation_subquery))
    # for assertion in assertions:
    #     print(f"{assertion.object_uniprot and assertion.object_uniprot.uniprot}, {assertion.subject_uniprot and assertion.subject_uniprot.uniprot}")
    services.log_timestamp("Got Assertions GON")
    curies = [(assertion.object_uniprot.uniprot if assertion.object_uniprot else assertion.object_curie) for assertion in assertions]
    services.log_timestamp("Got Curies 1 GON")
    curies.extend([assertion.subject_uniprot.uniprot if assertion.subject_uniprot else assertion.subject_curie for assertion in assertions])
    services.log_timestamp("Got Curies 2 GON")
    normalized_nodes = services.get_normalized_nodes(curies)
    node_list = []
    for assertion in assertions:
        node_list.extend(assertion.get_other_node_kgx(normalized_nodes))
    services.log_timestamp("Got node_list GON")
    unique_curies = set([])
    nodes = []
    for node in node_list:
        if node[0] not in unique_curies:
            unique_curies.add(node[0])
            nodes.append(node)
    services.log_timestamp("Got nodes GON")
    with open('C:\\Users\\edgar\\source\\repos\\Issue92\\other_nodes.tsv', 'w') as outfile:
        for node in nodes:
            outfile.write('\t'.join(str(val) for val in node))
            outfile.write('\n')
    services.log_timestamp("End GON")


def get_edges():
    services.log_timestamp("Start GE")
    s = models.session()
    evaluation_subquery = s.query(sqlalchemy.text('DISTINCT(assertion_id) FROM evaluation WHERE overall_correct = 0'))
    assertions = s.query(models.Assertion)\
        .options(joinedload(models.Assertion.evidence_list))\
        .filter(models.Assertion.assertion_id.notin_(evaluation_subquery))
    services.log_timestamp("Got Assertions GE")
    edges = []
    for assertion in assertions:
        edges.extend(assertion.get_edges_kgx())
    services.log_timestamp("Got edges GE")
    with open('C:\\Users\\edgar\\source\\repos\\Issue92\\edges.tsv', 'w') as outfile:
        for edge in edges:
            outfile.write('\t'.join(str(val) for val in edge))
            outfile.write('\n')
    services.log_timestamp("Wrote edges GE")
    other_edges = []
    for assertion in assertions:
        other_edges.extend(assertion.get_other_edges_kgx())
    services.log_timestamp("Got other edges GE")
    with open('C:\\Users\\edgar\\source\\repos\\Issue92\\other_edges.tsv', 'w') as outfile:
        for edge in other_edges:
            outfile.write('\t'.join(str(val) for val in edge))
            outfile.write('\n')
    services.log_timestamp("Wrote other edges GE")
    services.log_timestamp("End GE")


if __name__ == "__main__":
    services.log_timestamp("Start Main")
    get_nodes()
    get_other_nodes()
    get_edges()
    services.log_timestamp("End Main")
    # services.get_normalized_nodes(["MESH:D014867", "NCIT:C34373"])
