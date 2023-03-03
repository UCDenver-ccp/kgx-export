import json
import math
from math import fsum

import pymysql.connections
from google.cloud.sql.connector import Connector
from sqlalchemy import Column, String, Integer, Boolean, Float, DateTime, Text, ForeignKey, UniqueConstraint, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker

Model = declarative_base(name='Model')
session = None
HUMAN_TAXON = 'NCBITaxon:9606'


class Assertion(Model):
    __tablename__ = 'assertion'
    assertion_id = Column(String(65), primary_key=True)
    subject_curie = Column(String(100), ForeignKey('pr_to_uniprot.pr'))
    object_curie = Column(String(100), ForeignKey('pr_to_uniprot.pr'))
    association_curie = Column(String(100))
    evidence_list = relationship('Evidence', back_populates='assertion', lazy='subquery',
                                 primaryjoin='and_(Assertion.assertion_id==Evidence.assertion_id, '
                                             'Evidence.superseded_by.is_(None))')
    subject_uniprot = relationship('PRtoUniProt', foreign_keys=subject_curie, lazy='joined')
    object_uniprot = relationship('PRtoUniProt', foreign_keys=object_curie, lazy='joined')
    subject_idf = relationship('ConceptIDF', viewonly=True, foreign_keys=subject_curie, lazy='subquery',
                               primaryjoin='and_(Assertion.subject_curie==ConceptIDF.concept_curie, '
                                           'ConceptIDF.level=="document")')
    object_idf = relationship('ConceptIDF', viewonly=True, foreign_keys=object_curie, lazy='subquery',
                              primaryjoin='and_(Assertion.object_curie==ConceptIDF.concept_curie, '
                                          'ConceptIDF.level=="document")')

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
        relevant_scores = [evidence.get_score(self.subject_idf.idf if self.subject_idf else None,
                                              self.object_idf.idf if self.object_idf else None) for evidence in
                           self.evidence_list if evidence.get_top_predicate() == predicate]
        return fsum(relevant_scores) / float(len(relevant_scores))

    def get_node_kgx(self, normalized_nodes) -> list:
        object_name = 'UNKNOWN_NAME'
        object_category = 'biolink:NamedThing'
        subject_name = 'UNKNOWN_NAME'
        subject_category = 'biolink:NamedThing'
        if self.object_curie in normalized_nodes and normalized_nodes[self.object_curie] is not None:
            object_name = normalized_nodes[self.object_curie]['id']['label'] if 'label' in normalized_nodes[self.object_curie]['id'] else self.object_curie
            object_category = normalized_nodes[self.object_curie]['type'][0]
        if self.subject_curie in normalized_nodes and normalized_nodes[self.subject_curie] is not None:
            subject_name = normalized_nodes[self.subject_curie]['id']['label'] if 'label' in normalized_nodes[self.subject_curie]['id'] else self.subject_curie
            subject_category = normalized_nodes[self.subject_curie]['type'][0]
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
            object_category = normalized_nodes[object_id]['type'][0]
        if subject_id in normalized_nodes and normalized_nodes[subject_id] is not None:
            subject_name = normalized_nodes[subject_id]['id']['label'] if 'label' in normalized_nodes[subject_id]['id'] else subject_id
            subject_category = normalized_nodes[subject_id]['type'][0]
        return [[object_id, object_name, object_category],
                [subject_id, subject_name, subject_category]]

    def get_edges_kgx(self, limit=0) -> list:
        return [self.get_edge_kgx(predicate, limit) for predicate in self.get_predicates()]

    def get_edge_kgx(self, predicate, limit=0) -> list:
        relevant_evidence = [ev for ev in self.evidence_list if ev.get_top_predicate() == predicate]
        evidence_count = len(relevant_evidence)
        if limit > 0:
            relevant_evidence = relevant_evidence[:limit]
        supporting_study_results = '|'.join([f'tmkp:{ev.evidence_id}' for ev in relevant_evidence])
        supporting_publications = '|'.join([ev.document_id for ev in relevant_evidence])
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
            object_aspect_qualifier = 'biolink:activity_or_abundance'
            object_direction_qualifier = 'biolink:increased'
        elif predicate == 'biolink:entity_negatively_regulates_entity':
            predicate = 'biolink:affects'
            qualified_predicate = 'biolink:causes'
            object_aspect_qualifier = 'biolink:activity_or_abundance'
            object_direction_qualifier = 'biolink:decreased'
        elif predicate == 'biolink:gain_of_function_contributes_to':
            predicate = 'biolink:affects'
            qualified_predicate = 'biolink:contributes_to'
            subject_form_or_variant_qualifier = 'biolink:gain_of_function_variant_form'
        elif predicate == 'biolink:loss_of_function_contributes_to':
            predicate = 'biolink:affects'
            qualified_predicate = 'biolink:causes'
            subject_form_or_variant_qualifier = 'biolink:loss_of_function_variant_form'
        return [self.subject_curie, predicate, self.object_curie, qualified_predicate,
                subject_aspect_qualifier, subject_direction_qualifier,
                subject_part_qualifier, subject_form_or_variant_qualifier,
                object_aspect_qualifier, object_direction_qualifier,
                object_part_qualifier, object_form_or_variant_qualifier,
                anatomical_context_qualifier,
                self.assertion_id, self.association_curie, self.get_aggregate_score(predicate),
                supporting_study_results, supporting_publications,
                self.get_json_attributes(predicate, relevant_evidence, evidence_count)]

    def get_other_edges_kgx(self, limit=0) -> list:
        return [self.get_other_edge_kgx(predicate, limit) for predicate in self.get_predicates()]

    def get_other_edge_kgx(self, predicate, limit=0):
        if (self.object_curie.startswith('PR:') and not self.object_uniprot) or (self.subject_curie.startswith('PR:') and not self.subject_uniprot):
            return []
        if (self.subject_uniprot and not self.subject_uniprot.taxon == HUMAN_TAXON) or (self.object_uniprot and not self.object_uniprot.taxon == HUMAN_TAXON):
            return []
        subject_id = self.subject_uniprot.uniprot if self.subject_uniprot else self.subject_curie
        object_id = self.object_uniprot.uniprot if self.object_uniprot else self.object_curie
        relevant_evidence = [ev for ev in self.evidence_list if ev.get_top_predicate() == predicate]
        relevant_evidence.sort(key=lambda ev: 1 if ev.semmed_lookup else 0, reverse=True)
        relevant_evidence.sort(key=lambda ev: ev.get_score(self.subject_idf.idf if self.subject_idf else None, self.object_idf.idf if self.object_idf else None, predicate), reverse=True)
        evidence_count = len(relevant_evidence)
        if limit > 0:
            relevant_evidence = relevant_evidence[:limit]
        supporting_study_results = '|'.join([f'tmkp:{ev.evidence_id}' for ev in relevant_evidence])
        supporting_publications = '|'.join([ev.document_id for ev in relevant_evidence])
        return [subject_id, predicate, object_id, self.assertion_id, self.association_curie,
                self.get_aggregate_score(predicate), supporting_study_results, supporting_publications,
                self.get_json_attributes(predicate, relevant_evidence, evidence_count)]

    def get_json_attributes(self, predicate, evidence_list, evidence_count=0) -> json:
        if evidence_count == 0:
            evidence_count = len(evidence_list)
        semmed_count = 0
        for evidence in evidence_list:
            if evidence.semmed_lookup:
                semmed_count += 1
        attributes_list = [
            {
                "attribute_type_id": "biolink:original_knowledge_source",
                "value": "infores:text-mining-provider-targeted",
                "value_type_id": "biolink:InformationResource",
                # "description": "The Text Mining Provider Targeted Biolink Association KP from NCATS Translator provides text-mined assertions from the biomedical literature.",
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
                "value": evidence_count,
                "value_type_id": "biolink:EvidenceCount",
                # "description": "The count of the number of sentences that assert this edge",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:tmkp_confidence_score",
                "value": self.get_aggregate_score(predicate),
                "value_type_id": "biolink:ConfidenceLevel",
                # "description": "An aggregate confidence score that combines evidence from all sentences that support the edge",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:supporting_document",
                "value": '|'.join([ev.document_id for ev in evidence_list]),
                "value_type_id": "biolink:Publication",
                # "description": "The document(s) that contains the sentence(s) that assert the Biolink association represented by the edge; pipe-delimited",
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
        for study in evidence_list:
            attributes_list.append(study.get_json_attributes(self.subject_idf.idf if self.subject_idf else None,
                                                             self.object_idf.idf if self.object_idf else None))
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
    evidence_id = Column(String(65), ForeignKey('evidence.evidence_id'))
    overall_correct = Column(Boolean)
    subject_correct = Column(Boolean)
    object_correct = Column(Boolean)
    predicate_correct = Column(Boolean)
    source_id = Column(Integer)
    create_datetime = Column(DateTime)
    comments = Column(Text)

    def __init__(self, evidence_id, overall_correct, subject_correct, object_correct, predicate_correct, source_id, create_datetime, comments):
        self.evidence_id = evidence_id
        self.overall_correct = overall_correct
        self.subject_correct = subject_correct
        self.object_correct = object_correct
        self.predicate_correct = predicate_correct
        self.source_id = source_id
        self.create_datetime = create_datetime
        self.comments = comments


class Evidence(Model):
    __tablename__ = 'evidence'
    evidence_id = Column(String(65), ForeignKey('tm_semmed.tm_id'), primary_key=True)
    assertion_id = Column(String(65), ForeignKey('assertion.assertion_id'))
    assertion = relationship('Assertion', back_populates='evidence_list')
    document_id = Column(String(45), ForeignKey('document_year.document_id'))
    # document_id = Column(String(45), ForeignKey('document_year.document_id'), ForeignKey('semmed.pmid'))
    sentence = Column(String(2000))
    # sentence = Column(String(2000), ForeignKey('semmed.sentence'))
    subject_entity_id = Column(String(65), ForeignKey('entity.entity_id'))
    subject_entity = relationship('Entity', foreign_keys=subject_entity_id, lazy='joined')
    object_entity_id = Column(String(65), ForeignKey('entity.entity_id'))
    object_entity = relationship('Entity', foreign_keys=object_entity_id, lazy='joined')
    document_zone = Column(String(45))
    document_publication_type = Column(String(100))
    document_year_published = Column(Integer)
    actual_year = relationship('DocumentYear', foreign_keys=document_id, lazy='joined')
    superseded_by = Column(String(20))
    evidence_scores = relationship('EvidenceScore', lazy='subquery')
    semmed_lookup = relationship('TmSemmed', viewonly=True, uselist=True, foreign_keys=evidence_id, lazy='joined')

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

    def get_top_predicate(self) -> str:
        self.evidence_scores.sort(key=lambda ev_score: ev_score.score, reverse=True)
        return self.evidence_scores[0].predicate_curie

    def get_predicates(self) -> set:
        return set(es.predicate_curie for es in self.evidence_scores)

    def get_score(self, subject_idf=None, object_idf=None, predicate=None) -> float:
        if predicate is None:
            predicate = self.get_top_predicate()
        base_score = next((x.score for x in self.evidence_scores if x.predicate_curie == predicate), None)
        if not base_score:
            return 0.0
        if base_score and (not subject_idf or not object_idf):
            return base_score
        return abs(math.log10(subject_idf) * math.log10(object_idf) * base_score)

    def get_json_attributes(self, subject_idf=None, object_idf=None):
        nested_attributes = [
            {
                "attribute_type_id": "biolink:supporting_text",
                "value": self.sentence,
                "value_type_id": "EDAM:data_3671",
                # "description": "A sentence asserting the Biolink association represented by the parent edge",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:supporting_document",
                "value": self.document_id,
                "value_type_id": "biolink:Publication",
                "value_url": f"https://pubmed.ncbi.nlm.nih.gov/{str(self.document_id).split(':')[-1]}/",
                # "description": "The document that contains the sentence that asserts the Biolink association represented by the parent edge",
                "attribute_source": "infores:pubmed"
            },
            {
                "attribute_type_id": "biolink:supporting_text_located_in",
                "value": self.document_zone,
                "value_type_id": "IAO_0000314",
                # "description": "The part of the document where the sentence is located, e.g. title, abstract, introduction, conclusion, etc.",
                "attribute_source": "infores:pubmed"
            },
            {
                "attribute_type_id": "biolink:extraction_confidence_score",
                "value": self.get_score(subject_idf, object_idf),
                "value_type_id": "EDAM:data_1772",
                # "description": "The score provided by the underlying algorithm that asserted this sentence to represent the assertion specified by the parent edge",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:subject_location_in_text",
                "value": self.subject_entity.span if self.subject_entity else '',
                "value_type_id": "SIO:001056",
                # "description": "The start and end character offsets relative to the sentence for the subject of the assertion represented by the parent edge; start and end offsets are pipe-delimited, discontinuous spans are delimited using commas",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:object_location_in_text",
                "value": self.object_entity.span if self.object_entity else '',
                "value_type_id": "SIO:001056",
                # "description": "The start and end character offsets relative to the sentence for the object of the assertion represented by the parent edge; start and end offsets are pipe-delimited, discontinuous spans are delimited using commas",
                "attribute_source": "infores:text-mining-provider-targeted "
            }
        ]
        if self.actual_year:
            nested_attributes.append(
                {
                    "attribute_type_id": "biolink:supporting_document_year",
                    "value": self.actual_year.year,
                    "value_type_id": "UO:0000036",
                    # "description": "The year the document in which the sentence appears was published",
                    "attribute_source": "infores:pubmed"
                }
            )
        if self.semmed_lookup:
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
            "value": f"tmkp:{self.evidence_id}",
            "value_type_id": "biolink:TextMiningResult",
            # "description": "a single result from running NLP tool over a piece of text",
            "value_url": f"https://tmui.text-mining-kp.org/evidence/{self.evidence_id}",
            "attribute_source": "infores:text-mining-provider-targeted",
            "attributes": nested_attributes
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
    pr = Column(String(100), primary_key=True)
    uniprot = Column(String(100))
    taxon = Column(String(100))
    UniqueConstraint('pr', 'uniprot', name='pr+uniprot')

    def __init__(self, pr, uniprot, taxon):
        self.pr = pr
        self.uniprot = uniprot
        self.taxon = taxon


class ConceptIDF(Model):
    __tablename__ = 'concept_idf'
    concept_curie = Column(String(100), primary_key=True)
    level = Column(String(45), primary_key=True)
    idf = Column(Float)

    def __init__(self, concept_curie, level, idf):
        self.concept_curie = concept_curie
        self.level = level
        self.idf = idf


class PubmedToPMC(Model):
    __tablename__ = 'pubmed_to_pmc'
    pmid = Column(String(20), primary_key=True)
    pmcid = Column(String(20), primary_key=True)

    def __init__(self, pmid, pmcid):
        self.pmid = pmid
        self.pmcid = pmcid


class DocumentYear(Model):
    __tablename__ = 'document_year'
    document_id = Column(String(45), ForeignKey('evidence.document_id'), primary_key=True)
    year = Column(Integer)

    def __init__(self, document_id, year):
        self.document_id = document_id
        self.year = year


class TmSemmed(Model):
    __tablename__ = 'tm_semmed'
    tm_id = Column(String(65), ForeignKey(Evidence.evidence_id), primary_key=True)
    semmed_id = Column(Integer, primary_key=True)

    def __init__(self, tm_id, semmed_id):
        self.tm_id = tm_id
        self.semmed_id = semmed_id


def init_db(instance: str, user: str, password: str, database: str) -> None:  # pragma: no cover
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
    global session
    session = sessionmaker()
    session.configure(bind=engine)
