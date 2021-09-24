import os
import json
import logging
import pymysql.connections
from sqlalchemy import Column, String, Integer, Boolean, Float, ForeignKey, UniqueConstraint, create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from math import fsum
from google.cloud.sql.connector import connector

Model = declarative_base(name='Model')
session = None


class Assertion(Model):
    __tablename__ = 'assertion'
    assertion_id = Column(String(65), primary_key=True)
    subject_curie = Column(String(100), ForeignKey('pr_to_uniprot.pr'))
    object_curie = Column(String(100), ForeignKey('pr_to_uniprot.pr'))
    association_curie = Column(String(100))
    evidence_list = relationship('Evidence', back_populates='assertion')
    subject_uniprot = relationship('PRtoUniProt', foreign_keys=subject_curie, lazy='joined')
    object_uniprot = relationship('PRtoUniProt', foreign_keys=object_curie, lazy='joined')

    def __init__(self, assertion_id, subject_curie, object_curie, association):
        self.assertion_id = assertion_id
        self.subject_curie = subject_curie
        self.object_curie = object_curie
        self.association_curie = association

    def get_predicate_scores(self) -> dict:
        predicate_scores_dict = {}
        for predicate in self.get_predicates():
            predicate_scores_dict[predicate] = self.get_aggregate_score(predicate)
        return predicate_scores_dict

    def get_predicates(self) -> set:
        return set(evidence.get_top_predicate() for evidence in self.evidence_list)

    def get_aggregate_score(self, predicate) -> float:
        relevant_scores = [evidence.get_score() for evidence in self.evidence_list if evidence.get_top_predicate() == predicate]
        return fsum(relevant_scores) / float(len(relevant_scores))

    def get_node_kgx(self, normalized_nodes) -> list:
        object_name = 'UNKNOWN_NAME'
        object_category = 'biolink:NamedThing'
        subject_name = 'UNKNOWN_NAME'
        subject_category = 'biolink:NamedThing'
        if self.object_curie in normalized_nodes and normalized_nodes[self.object_curie] is not None:
            object_name = normalized_nodes[self.object_curie]['id']['label'] if 'label' in normalized_nodes[self.object_curie]['id'] else self.object_curie
            object_category = 'biolink:ChemicalEntity' if self.object_curie.startswith('CHEBI') else 'biolink:Protein'  # normalized_nodes[object_id]['type'][0]
        if self.subject_curie in normalized_nodes and normalized_nodes[self.subject_curie] is not None:
            subject_name = normalized_nodes[self.subject_curie]['id']['label'] if 'label' in normalized_nodes[self.subject_curie]['id'] else self.subject_curie
            subject_category = 'biolink:ChemicalEntity' if self.subject_curie.startswith('CHEBI') else 'biolink:Protein'  # normalized_nodes[subject_id]['type'][0]
        return [[self.object_curie, object_name, object_category],
                [self.subject_curie, subject_name, subject_category]]

    def get_uniprot_node_kgx(self, normalized_nodes) -> list:
        if (self.object_curie.startswith('PR:') and not self.object_uniprot) or (self.subject_curie.startswith('PR:') and not self.subject_uniprot):
            return []
        object_id = self.object_uniprot.uniprot if self.object_uniprot else self.object_curie
        object_name = 'UNKNOWN_NAME'
        object_category = 'biolink:NamedThing'
        subject_id = self.subject_uniprot.uniprot if self.subject_uniprot else self.subject_curie
        subject_name = 'UNKNOWN_NAME'
        subject_category = 'biolink:NamedThing'

        if object_id in normalized_nodes and normalized_nodes[object_id] is not None:
            object_name = normalized_nodes[object_id]['id']['label'] if 'label' in normalized_nodes[object_id]['id'] else object_id
            object_category = 'biolink:ChemicalEntity' if object_id.startswith('CHEBI') else 'biolink:Protein'  # normalized_nodes[object_id]['type'][0]
        if subject_id in normalized_nodes and normalized_nodes[subject_id] is not None:
            subject_name = normalized_nodes[subject_id]['id']['label'] if 'label' in normalized_nodes[subject_id]['id'] else subject_id
            subject_category = 'biolink:ChemicalEntity' if subject_id.startswith('CHEBI') else 'biolink:Protein'  # normalized_nodes[subject_id]['type'][0]
        return [[object_id, object_name, object_category],
                [subject_id, subject_name, subject_category]]

    def get_edges_kgx(self) -> list:
        return [self.get_edge_kgx(predicate) for predicate in self.get_predicates()]

    def get_edge_kgx(self, predicate) -> list:
        relevant_evidence = [ev for ev in self.evidence_list if ev.get_top_predicate() == predicate]
        supporting_study_results = '|'.join([f'tmkp:{ev.evidence_id}' for ev in relevant_evidence])
        supporting_publications = '|'.join([ev.document_id for ev in relevant_evidence])
        return [self.subject_curie, predicate, self.object_curie, self.assertion_id,
                self.association_curie, self.get_aggregate_score(predicate), supporting_study_results, supporting_publications,
                self.get_json_attributes(predicate, relevant_evidence)]

    def get_other_edges_kgx(self) -> list:
        return [self.get_other_edge_kgx(predicate) for predicate in self.get_predicates()]

    def get_other_edge_kgx(self, predicate):
        if (self.object_curie.startswith('PR:') and not self.object_uniprot) or (self.subject_curie.startswith('PR:') and not self.subject_uniprot):
            return []
        subject_id = self.subject_uniprot.uniprot if self.subject_uniprot else self.subject_curie
        object_id = self.object_uniprot.uniprot if self.object_uniprot else self.object_curie
        relevant_evidence = [ev for ev in self.evidence_list if ev.get_top_predicate() == predicate]
        supporting_study_results = '|'.join([f'tmkp:{ev.evidence_id}' for ev in relevant_evidence])
        supporting_publications = '|'.join([ev.document_id for ev in relevant_evidence])
        return [subject_id, predicate, object_id, self.assertion_id, self.association_curie,
                self.get_aggregate_score(predicate), supporting_study_results, supporting_publications,
                self.get_json_attributes(predicate, relevant_evidence)]

    def get_json_attributes(self, predicate, evidence_list) -> json:
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
                "value": "infores:pubmed",
                "value_type_id": "biolink:InformationResource",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:has_evidence_count",
                "value": len(evidence_list),
                "value_type_id": "biolink:EvidenceCount",
                "description": "The count of the number of sentences that assert this edge",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:tmkp_confidence_score",
                "value": self.get_aggregate_score(predicate),
                "value_type_id": "biolink:ConfidenceLevel",
                "description": "An aggregate confidence score that combines evidence from all sentences that support the edge",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:supporting_document",
                "value": '|'.join([ev.document_id for ev in evidence_list]),
                "value_type_id": "biolink:Publication",
                "description": "The document(s) that contains the sentence(s) that assert the Biolink association represented by the edge; pipe-delimited",
                "attribute_source": "infores:pubmed"
            }
        ]
        for study in evidence_list:
            attributes_list.append(study.get_json_attributes())
        return json.dumps(attributes_list)


class Entity(Model):
    __tablename__ = 'entity'
    entity_id = Column(String(65), primary_key=True)
    span = Column(String(45))
    covered_text = Column(String(100))

    def __init__(self, entity_id, span, covered_text):
        self.entity_id = entity_id
        self.span = span
        self.covered_text = covered_text


class Evaluation(Model):
    __tablename__ = 'evaluation'
    id = Column(Integer, primary_key=True)
    assertion_id = Column(String(65), ForeignKey('assertion.assertion_id'))
    overall_correct = Column(Boolean)
    subject_correct = Column(Boolean)
    object_correct = Column(Boolean)
    predicate_correct = Column(Boolean)
    api_keys_id = Column(Integer)

    def __init__(self, assertion_id, overall_correct, subject_correct, object_correct, predicate_correct, api_keys_id):
        self.assertion_id = assertion_id
        self.overall_correct = overall_correct
        self.subject_correct = subject_correct
        self.object_correct = object_correct
        self.predicate_correct = predicate_correct
        self.api_keys_id = api_keys_id


class Evidence(Model):
    __tablename__ = 'evidence'
    evidence_id = Column(String(65), primary_key=True)
    assertion_id = Column(String(65), ForeignKey('assertion.assertion_id'))
    assertion = relationship('Assertion', back_populates='evidence_list')
    document_id = Column(String(45))
    sentence = Column(String(2000))
    subject_entity_id = Column(String(65), ForeignKey('entity.entity_id'))
    subject_entity = relationship('Entity', foreign_keys=subject_entity_id, lazy='joined')
    object_entity_id = Column(String(65), ForeignKey('entity.entity_id'))
    object_entity = relationship('Entity', foreign_keys=object_entity_id, lazy='joined')
    document_zone = Column(String(45))
    document_publication_type = Column(String(100))
    document_year_published = Column(Integer)
    evidence_scores = relationship('EvidenceScore', lazy='joined')

    def __init__(self, evidence_id, assertion_id, document_id, sentence, subject_entity_id, object_entity_id,
                 document_zone, document_publication_type, document_year_published):
        self.evidence_id = evidence_id
        self.assertion_id = assertion_id
        self.document_id = document_id
        self.sentence = sentence
        self.subject_entity_id = subject_entity_id
        self.object_entity_id = object_entity_id
        self.document_zone = document_zone
        self.document_publication_type = document_publication_type
        self.document_year_published = document_year_published

    def get_top_predicate(self) -> str:
        self.evidence_scores.sort(key=lambda ev_score: ev_score.score, reverse=True)
        return self.evidence_scores[0].predicate_curie

    def get_predicates(self) -> set:
        return set(es.predicate_curie for es in self.evidence_scores)

    def get_score(self, predicate=None) -> float:
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
                    "attribute_type_id": "biolink:supporting_text",
                    "value": self.sentence,
                    "value_type_id": "EDAM:data_3671",
                    "description": "A sentence asserting the Biolink association represented by the parent edge",
                    "attribute_source": "infores:text-mining-provider-targeted"
                },
                {
                    "attribute_type_id": "biolink:supporting_document",
                    "value": self.document_id,
                    "value_type_id": "biolink:Publication",
                    "value_url": f"https://pubmed.ncbi.nlm.nih.gov/{str(self.document_id).split(':')[-1]}/",
                    "description": "The document that contains the sentence that asserts the Biolink association represented by the parent edge",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:supporting_document_type",
                    "value": self.document_publication_type,
                    "value_type_id": "MESH:U000020",
                    "description": "The publication type(s) for the document in which the sentence appears, as defined by PubMed; pipe-delimited",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:supporting_document_year",
                    "value": self.document_year_published,
                    "value_type_id": "UO:0000036",
                    "description": "The year the document in which the sentence appears was published",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:supporting_text_located_in",
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
                    "attribute_type_id": "biolink:subject_location_in_text",
                    "value": self.subject_entity.span,
                    "value_type_id": "SIO:001056",
                    "description": "The start and end character offsets relative to the sentence for the subject of the assertion represented by the parent edge; start and end offsets are pipe-delimited, discontinuous spans are delimited using commas",
                    "attribute_source": "infores:text-mining-provider-targeted"
                },
                {
                    "attribute_type_id": "biolink:object_location_in_text",
                    "value": self.object_entity.span,
                    "value_type_id": "SIO:001056",
                    "description": "The start and end character offsets relative to the sentence for the object of the assertion represented by the parent edge; start and end offsets are pipe-delimited, discontinuous spans are delimited using commas",
                    "attribute_source": "infores:text-mining-provider-targeted "
                }
            ]
        }


class EvidenceScore(Model):
    __tablename__ = 'evidence_score'
    evidence_id = Column(String(65), ForeignKey('evidence.evidence_id'), primary_key=True)
    predicate_curie = Column(String(100), primary_key=True)
    score = Column(Float)
    UniqueConstraint('evidence_id', 'predicate_curie', name='evidence+predicate')

    def __init__(self, evidence_id, predicate_curie, score):
        self.evidence_id = evidence_id
        self.predicate_curie = predicate_curie
        self.score = score


class PRtoUniProt(Model):
    __tablename__ = 'pr_to_uniprot'
    pr = Column(String(100), ForeignKey('cooccurrence.entity1_curie'), ForeignKey('cooccurrence.entity2_curie'), primary_key=True)
    uniprot = Column(String(100))
    UniqueConstraint('pr', 'uniprot', name='pr+uniprot')

    def __init__(self, pr, uniprot):
        self.pr = pr
        self.uniprot = uniprot


class Cooccurrence(Model):
    __tablename__ = 'cooccurrence'
    cooccurrence_id = Column(String(27), primary_key=True)
    entity1_curie = Column(String(100))
    entity1_uniprot = relationship('PRtoUniProt', viewonly=True, uselist=False, primaryjoin='Cooccurrence.entity1_curie == PRtoUniProt.pr')
    entity2_curie = Column(String(45))
    entity2_uniprot = relationship('PRtoUniProt', viewonly=True, uselist=False, primaryjoin='Cooccurrence.entity2_curie == PRtoUniProt.pr')
    scores_list = relationship('CooccurrenceScores', primaryjoin='Cooccurrence.cooccurrence_id == CooccurrenceScores.cooccurrence_id')

    def get_edge_kgx(self, use_uniprot=False) -> list:
        if use_uniprot:
            if (self.entity1_curie.startswith('PR:') and not self.entity1_uniprot) or (self.entity2_curie.startswith('PR:') and not self.entity2_uniprot):
                return []
        entity1 = self.entity1_uniprot if use_uniprot and self.entity1_uniprot else self.entity1_curie
        entity2 = self.entity2_uniprot if use_uniprot and self.entity2_uniprot else self.entity2_curie
        supporting_study_results = '|'.join(f'tmkp:{sco.cooccurrence_id}_{sco.level}' for sco in self.scores_list)
        return [entity1, 'biolink:related_to', entity2, self.cooccurrence_id,
                'biolink:Association', supporting_study_results, json.dumps(self.get_json_attributes())]

    def get_json_attributes(self) -> list:
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
            }
        ]
        for score in self.scores_list:
            attributes_list.append(score.get_json_attributes())
        return attributes_list


class CooccurrenceScores(Model):
    __tablename__ = 'cooccurrence_scores'
    cooccurrence_id = Column(String(27), ForeignKey('cooccurrence.cooccurrence_id'), ForeignKey('cooccurrence_publication.cooccurrence_id'), primary_key=True)
    level = Column(String(45), ForeignKey('cooccurrence_publication.level'), primary_key=True)
    publication_list = relationship('CooccurrencePublication', viewonly=True, uselist=True,
                                    primaryjoin='and_(CooccurrenceScores.cooccurrence_id == CooccurrencePublication.cooccurrence_id, CooccurrenceScores.level == CooccurrencePublication.level)')
    concept1_count = Column(Integer)
    concept2_count = Column(Integer)
    pair_count = Column(Integer)
    ngd = Column(Float)
    pmi = Column(Float)
    pmi_norm = Column(Float)
    pmi_norm_max = Column(Float)
    mutual_dependence = Column(Float)
    lfmd = Column(Float)

    def get_json_attributes(self) -> dict:
        biolink_level = 'biolink:DocumentLevelConceptCooccurrenceAnalysisResult'
        desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur at the document level'
        if self.level == 'document':
            biolink_level = 'biolink:DocumentLevelConceptCooccurrenceAnalysisResult'
            desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur at the document level'
        elif self.level == 'sentence':
            biolink_level = 'biolink:SentenceLevelConceptCooccurrenceAnalysisResult'
            desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur at the sentence level'
        elif self.level == 'title':
            biolink_level = 'biolink:TitleLevelConceptCooccurrenceAnalysisResult'
            desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur in the document title'
        elif self.level == 'abstract':
            biolink_level = 'biolink:AbstractLevelConceptCooccurrenceAnalysisResult'
            desc = 'a single result from computing cooccurrence metrics between two concepts that cooccur in the abstract'
        return {
            "attribute_type_id": "biolink:supporting_study_result",
            "value": f"tmkp:{self.cooccurrence_id}_{self.level}",
            "value_type_id": biolink_level,
            "description": desc,
            "attribute_source": "infores:text-mining-provider-cooccurrence",
            "attributes": [
                {
                    "attribute_type_id": "biolink:supporting_document",
                    "value": '|'.join(f'tmkp:{pub.document_id}' for pub in self.publication_list),
                    "value_type_id": "biolink:Publication",
                    "description": f"The documents where the concepts of this assertion were observed to cooccur at the {self.level} level.",
                    "attribute_source": "infores:pubmed"
                },
                {
                    "attribute_type_id": "biolink:tmkp_concept1_count",
                    "value": self.concept1_count,
                    "value_type_id": "SIO:000794",
                    "description": f"The number of times concept #1 was observed to occur at the {self.level} level in the documents that were processed"
                },
                {
                    "attribute_type_id": "biolink:tmkp_concept2_count",
                    "value": self.concept2_count,
                    "value_type_id": "SIO:000794",
                    "description": f"The number of times concept #2 was observed to occur at the {self.level} level in the documents that were processed"
                },
                {
                    "attribute_type_id": "biolink:tmkp_concept_pair_count",
                    "value": self.pair_count,
                    "value_type_id": "SIO:000794",
                    "description": f"The number of times the concepts of this assertion were observed to cooccur at the {self.level} level in the documents that were processed"
                },
                {
                    "attribute_type_id": "biolink:tmkp_normalized_google_distance",
                    "value": self.ngd,
                    "value_type_id": "EDAM:data_1772",
                    "description": f"The number of times the concepts of this assertion were observed to cooccur at the {self.level} level in the documents that were processed",
                    "attribute_source": "infores:text-mining-provider-cooccurrence"
                },
                {
                    "attribute_type_id": "biolink:tmkp_pointwise_mutual_information",
                    "value": self.pmi,
                    "value_type_id": "EDAM:data_1772",
                    "description": "The pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                    "attribute_source": "infores:text-mining-provider-cooccurrence"
                },
                {
                    "attribute_type_id": "biolink:tmkp_normalized_pointwise_mutual_information",
                    "value": self.pmi_norm,
                    "value_type_id": "EDAM:data_1772",
                    "description": "The normalized pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                    "attribute_source": "infores:text-mining-provider-cooccurrence"
                },
                {
                    "attribute_type_id": "biolink:tmkp_mutual_dependence",
                    "value": self.mutual_dependence,
                    "value_type_id": "EDAM:data_1772",
                    "description": "The mutual dependence (PMI^2) score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                    "attribute_source": "infores:text-mining-provider-cooccurrence"
                },
                {
                    "attribute_type_id": "biolink:tmkp_normalized_pointwise_mutual_information_max",
                    "value": self.pmi_norm_max,
                    "value_type_id": "EDAM:data_1772",
                    "description": "A variant of the normalized pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                    "attribute_source": "infores:text-mining-provider-cooccurrence"
                },
                {
                    "attribute_type_id": "biolink:tmkp_log_frequency_biased_mutual_dependence",
                    "value": self.lfmd,
                    "value_type_id": "EDAM:data_1772",
                    "description": "The log frequency biased mutual dependence score for the concepts in this assertion based on their cooccurrence in the documents that were processed",
                    "attribute_source": "infores:text-mining-provider-cooccurrence"
                }
            ]
        }


class CooccurrencePublication(Model):
    __tablename__ = 'cooccurrence_publication'
    cooccurrence_id = Column(String(27), ForeignKey('cooccurrence_scores.cooccurrence_id'), primary_key=True)
    level = Column(String(45), ForeignKey('cooccurrence_scores.level'), primary_key=True)
    document_id = Column(String(45), primary_key=True)


class ConceptIDF(Model):
    __tablename__ = 'concept_idf'
    concept_curie = Column(String(100), primary_key=True)
    level = Column(String(45), primary_key=True)
    idf = Column(Float)


def init_db(instance, user, password, database):
    def get_conn() -> pymysql.connections.Connection:
        conn: pymysql.connections.Connection = connector.connect(
            instance_connection_string=instance,
            driver='pymysql',
            user=user,
            password=password,
            database=database
        )
        return conn
    engine = create_engine('mysql+pymysql://', creator=get_conn)
    global session
    session = sessionmaker()
    session.configure(bind=engine)
