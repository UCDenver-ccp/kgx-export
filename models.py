import os
import json
from sqlalchemy import Column, String, Integer, Boolean, Float, ForeignKey, UniqueConstraint, create_engine, engine, func
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from math import fsum

Model = declarative_base(name='Model')

# TODO: Fill in the String/VARCHAR lengths on the appropriate column definitions.


class Assertion(Model):
    __tablename__ = 'assertion'
    id = Column(Integer, primary_key=True)
    assertion_id = Column(String, primary_key=True)
    subject_curie = Column(String, ForeignKey('pr_to_uniprot.pr'))
    object_curie = Column(String, ForeignKey('pr_to_uniprot.pr'))
    association_curie = Column(String)
    evidence_list = relationship('Evidence', back_populates='assertion')
    subject_uniprot = relationship('PRtoUniProt', foreign_keys=subject_curie, lazy='joined')
    object_uniprot = relationship('PRtoUniProt', foreign_keys=object_curie, lazy='joined')
    # evaluation_list = relationship('Evaluation', back_populates='assertion')

    def test_summary(self):
        print(self.assertion_id)
        for (predicate, score) in self.get_predicate_scores().items():
            print(f"({predicate}, {score}")

    def get_predicate_scores(self):
        predicate_scores_dict = {}
        for predicate in self.get_predicates():
            predicate_scores_dict[predicate] = self.get_aggregate_score(predicate)
        return predicate_scores_dict

    # def is_correct(self):
    #     if len(self.evaluation_list) > 0:
    #         for evaluation in self.evaluation_list:
    #             if not evaluation.overall_correct:
    #                 return False
    #     return True

    def get_predicates(self):
        return set(evidence.get_top_predicate() for evidence in self.evidence_list)

    def get_aggregate_score(self, predicate):
        relevant_scores = [evidence.get_score() for evidence in self.evidence_list if evidence.get_top_predicate() == predicate]
        return fsum(relevant_scores) / float(len(relevant_scores))

    def get_node_kgx(self, normalized_nodes):
        object_name = 'UNKNOWN_NAME'
        object_category = 'biolink:NamedThing'
        subject_name = 'UNKNOWN_NAME'
        subject_category = 'biolink:NamedThing'
        if self.object_curie in normalized_nodes and normalized_nodes[self.object_curie] is not None:
            object_name = normalized_nodes[self.object_curie]['id']['label']
            object_category = normalized_nodes[self.object_curie]['type'][0]
        if self.subject_curie in normalized_nodes and normalized_nodes[self.subject_curie] is not None:
            subject_name = normalized_nodes[self.subject_curie]['id']['label']
            subject_category = normalized_nodes[self.subject_curie]['type'][0]
        return [[self.object_curie, object_name, object_category],
                [self.subject_curie, subject_name, subject_category]]

    def get_other_node_kgx(self, normalized_nodes):
        object_id = self.object_uniprot.uniprot if self.object_uniprot else self.object_curie
        object_name = 'UNKNOWN_NAME'
        object_category = 'biolink:NamedThing'
        subject_id = self.subject_uniprot.uniprot if self.subject_uniprot else self.subject_curie
        subject_name = 'UNKNOWN_NAME'
        subject_category = 'biolink:NamedThing'

        if object_id in normalized_nodes and normalized_nodes[object_id] is not None:
            object_name = normalized_nodes[object_id]['id']['label']
            object_category = normalized_nodes[object_id]['type'][0]
        if subject_id in normalized_nodes and normalized_nodes[subject_id] is not None:
            subject_name = normalized_nodes[subject_id]['id']['label']
            subject_category = normalized_nodes[subject_id]['type'][0]
        return [[object_id, object_name, object_category],
                [subject_id, subject_name, subject_category]]

    def get_edges_kgx(self):
        return [self.get_edge_kgx(predicate) for predicate in self.get_predicates()]

    def get_edge_kgx(self, predicate):
        relevant_evidence = [ev for ev in self.evidence_list if ev.get_top_predicate() == predicate]
        supporting_study_results = '|'.join([f'tmkp:{ev.evidence_id}' for ev in relevant_evidence])
        supporting_publications = '|'.join([ev.document_id for ev in relevant_evidence])
        return [self.subject_curie, predicate, self.object_curie, self.assertion_id,
                self.association_curie, self.get_aggregate_score(predicate), supporting_study_results, supporting_publications,
                self.get_json_attributes(predicate, relevant_evidence)]

    def get_other_edges_kgx(self):
        return [self.get_other_edge_kgx(predicate) for predicate in self.get_predicates()]

    def get_other_edge_kgx(self, predicate):
        subject_id = self.subject_uniprot.uniprot if self.subject_uniprot else self.subject_curie
        object_id = self.object_uniprot.uniprot if self.object_uniprot else self.object_curie
        relevant_evidence = [ev for ev in self.evidence_list if ev.get_top_predicate() == predicate]
        supporting_study_results = '|'.join([f'tmkp:{ev.evidence_id}' for ev in relevant_evidence])
        supporting_publications = '|'.join([ev.document_id for ev in relevant_evidence])
        return [subject_id, predicate, object_id, self.assertion_id, self.association_curie,
                self.get_aggregate_score(predicate), supporting_study_results, supporting_publications,
                self.get_json_attributes(predicate, relevant_evidence)]

    def get_json_attributes(self, predicate, evidence_list):
        attributes_list = [
            {
                "attribute_type_id": "biolink:original_knowledge_source",
                "value": "infores:text-mining-provider-targeted",
                "value_type_id": "biolink:InformationResource",
                "description": "The Text Mining Provider Targeted Biolink Association KP from NCATS Translator provides text-mined assertions from the biomedical literature.",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:supporting_data_source",
                "value": "infores:pubmed",  # this will need to come from the db, eventually
                "value_type_id": "biolink:InformationResource",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:tmkp_confidence_score",
                "value": self.get_aggregate_score(predicate),
                "value_type_id": "biolink:ConfidenceLevel",
                "description": "An aggregate confidence score that combines evidence from all sentences that support the edge",
                "attribute_source": "infores:text-mining-provider-targeted"
            }
        ]
        for study in evidence_list:
            attributes_list.append(study.get_json_attributes())
        return json.dumps(attributes_list)


class Entity(Model):
    __tablename__ = 'entity'
    entity_id = Column(String, primary_key=True)
    span = Column(String)
    covered_text = Column(String)


class Evaluation(Model):
    __tablename__ = 'evaluation'
    id = Column(Integer, primary_key=True)
    assertion_id = Column(String, ForeignKey('assertion.assertion_id'))
    # assertion = relationship('Assertion', back_populates='evaluation_list')
    overall_correct = Column(Boolean)
    subject_correct = Column(Boolean)
    object_correct = Column(Boolean)
    predicate_correct = Column(Boolean)
    api_keys_id = Column(Integer)


class Evidence(Model):
    __tablename__ = 'evidence'
    evidence_id = Column(String, primary_key=True)
    assertion_id = Column(String, ForeignKey('assertion.assertion_id'))
    assertion = relationship('Assertion', back_populates='evidence_list')
    document_id = Column(String)
    sentence = Column(String)
    subject_entity_id = Column(String, ForeignKey('entity.entity_id'))
    subject_entity = relationship('Entity', foreign_keys=subject_entity_id, lazy='joined')
    object_entity_id = Column(String, ForeignKey('entity.entity_id'))
    object_entity = relationship('Entity', foreign_keys=object_entity_id, lazy='joined')
    document_zone = Column(String)
    document_publication_type = Column(String)
    document_year_published = Column(Integer)
    evidence_scores = relationship('EvidenceScore', lazy='joined')

    def get_top_predicate(self):
        self.evidence_scores.sort(key=lambda ev_score: ev_score.score, reverse=True)
        return self.evidence_scores[0].predicate_curie

    def get_predicates(self):
        return set(es.predicate_curie for es in self.evidence_scores)

    def get_score(self, predicate=None):
        if predicate is None:
            predicate = self.get_top_predicate()
        return next((x.score for x in self.evidence_scores if x.predicate_curie == predicate), None)

    def get_json_attributes(self):
        return {
            "attribute_type_id": "biolink:supporting_study_result",
            "value": f"tmkp:{self.evidence_id}",
            "value_type_id": "biolink:TextMiningResult",
            "description": "a single result from running NLP tool over a piece of text",
            "attribute_source": "infores:text-mining-provider-targeted",
            "attributes": [
                {
                    "attribute_type_id": "biolink:source_text",
                    "value": self.sentence,
                    "value_type_id": "EDAM:data_3671",
                    "description": "The text that asserts the relationship between the subject and object entity",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:source_publication",
                    "value": self.document_id,
                    "value_type_id": "biolink:Publication",
                    "value_url": f"https://pubmed.ncbi.nlm.nih.gov/{str(self.document_id).split(':')[-1]}/",
                    "description": "The document that contains the sentence that asserts the Biolink association represented by the parent edge",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:source_publication_type",
                    "value": self.document_publication_type,
                    "value_type_id": "MESH:U000020",
                    "description": "The publication type(s) for the document in which the sentence appears, as defined by PubMed; pipe-delimited",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:source_publication_year",
                    "value": self.document_year_published,
                    "value_type_id": "UO:0000036",
                    "description": "The year the document in which the sentence appears was published",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:source_text_located_in",
                    "value": self.document_zone,
                    "value_type_id": "IAO_0000314",
                    "description": "The part of the document where the sentence is located, e.g. title, abstract, introduction, conclusion, etc.",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:extraction_confidence_score",
                    "value": self.get_score(),
                    "value_type_id": "EDAM:data_1772",
                    "description": "The score provided by the underlying algorithm that asserted this sentence to represent the assertion specified by the parent edge",
                    "attribute_source": "infores:text-mining-provider-targeted"
                },
                {
                    "attribute_type_id": "biolink:subject_text_location",
                    "value": self.subject_entity.span,
                    "value_type_id": "SIO:001056",
                    "description": "The start and end character offsets relative to the sentence for the subject of the assertion represented by the parent edge; start and end offsets are pipe-delimited, discontinuous spans are delimited using commas",
                    "attribute_source": "infores:text-mining-provider-targeted"
                },
                {
                    "attribute_type_id": "biolink:object_text_location",
                    "value": self.object_entity.span,
                    "value_type_id": "SIO:001056",
                    "description": "The start and end character offsets relative to the sentence for the object of the assertion represented by the parent edge; start and end offsets are pipe-delimited, discontinuous spans are delimited using commas",
                    "attribute_source": "infores:text-mining-provider-targeted "
                }
            ]
        }


class EvidenceScore(Model):
    __tablename__ = 'evidence_score'
    evidence_id = Column(String, ForeignKey('evidence.evidence_id'), primary_key=True)
    predicate_curie = Column(String, primary_key=True)
    score = Column(Float)
    UniqueConstraint('evidence_id', 'predicate_curie', name='evidence+predicate')


class PRtoUniProt(Model):
    __tablename__ = 'pr_to_uniprot'
    pr = Column(String, primary_key=True)
    uniprot = Column(String)
    UniqueConstraint('pr', 'uniprot', name='pr+uniprot')


username = os.getenv('MYSQL_DATABASE_USER', None)
secret_password = os.getenv('MYSQL_DATABASE_PASSWORD', None)
assert username
assert secret_password
url = engine.url.URL.create(
    drivername="mysql+pymysql",
    username=username,
    password=secret_password,
    database='text_mined_assertions',
    host='localhost',
    port=3306
    # query={
    #     "unix_socket": "/cloudsql/lithe-vault-265816:us-central1:text-mined-assertions-stage"
    # }
)
engine = create_engine(url, echo=False, future=True)
session = sessionmaker()
session.configure(bind=engine)
