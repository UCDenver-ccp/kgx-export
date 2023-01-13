import json
import math
from math import fsum

import pymysql.connections
from google.cloud.sql.connector import Connector
from sqlalchemy import Column, String, Integer, Boolean, Float, ForeignKey, UniqueConstraint, create_engine
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
                                 primaryjoin='and_(Assertion.assertion_id==Evidence.assertion_id, Evidence.superseded_by.is_(None))')
    subject_uniprot = relationship('PRtoUniProt', foreign_keys=subject_curie, lazy='joined')
    object_uniprot = relationship('PRtoUniProt', foreign_keys=object_curie, lazy='joined')
    subject_idf = relationship('ConceptIDF', viewonly=True, foreign_keys=subject_curie, lazy='subquery',
                               primaryjoin='and_(Assertion.subject_curie==ConceptIDF.concept_curie, ConceptIDF.level=="document")')
    object_idf = relationship('ConceptIDF', viewonly=True, foreign_keys=object_curie, lazy='subquery',
                              primaryjoin='and_(Assertion.object_curie==ConceptIDF.concept_curie, ConceptIDF.level=="document")')
    # updated_date = Column(DateTime)  # This will probably change soon

    def __init__(self, assertion_id, subject_curie, object_curie, association):
        self.assertion_id = assertion_id
        self.subject_curie = subject_curie
        self.object_curie = object_curie
        self.association_curie = association
        # self.updated_date = updated_date

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
        # display_predicate = predicate
        # if predicate == 'biolink:gain_of_function_contributes_to' or predicate == 'biolink:loss_of_function_contributes_to':
        #     display_predicate = 'biolink:contributes_to'
        return [self.subject_curie, predicate, self.object_curie, self.assertion_id,
                self.association_curie, self.get_aggregate_score(predicate), supporting_study_results, supporting_publications,
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
        # display_predicate = predicate
        # if predicate == 'biolink:gain_of_function_contributes_to' or predicate == 'biolink:loss_of_function_contributes_to':
        #     display_predicate = 'biolink:contributes_to'
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
        # if predicate == 'biolink:gain_of_function_contributes_to':
        #     attributes_list.append({
        #         "attribute_type_id": "biolink:sequence_variant_qualifier",
        #         "value": "SO:0002053",
        #         "value_type_id": "biolink:SequenceVariant",
        #         "attribute_source": "infores:text-mining-provider-targeted"
        #     })
        #     attributes_list.append({
        #         "attribute_type_id"
        #     })
        # if predicate == 'biolink:loss_of_function_contributes_to':
        #     attributes_list.append({
        #         "attribute_type_id": "biolink:sequence_variant_qualifier",
        #         "value": "SO:0002054",
        #         "value_type_id": "biolink:SequenceVariant",
        #         # "description": "Indicates that the gene in this assertion is a loss-of-function variant",
        #         "attribute_source": "infores:text-mining-provider-targeted"
        #     })
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
    pr = Column(String(100), ForeignKey('cooccurrence.entity1_curie'), ForeignKey('cooccurrence.entity2_curie'), primary_key=True)
    uniprot = Column(String(100))
    taxon = Column(String(100))
    UniqueConstraint('pr', 'uniprot', name='pr+uniprot')

    def __init__(self, pr, uniprot, taxon):
        self.pr = pr
        self.uniprot = uniprot
        self.taxon = taxon


class Cooccurrence(Model):
    __tablename__ = 'cooccurrence'
    cooccurrence_id = Column(String(27), primary_key=True)
    entity1_curie = Column(String(100))
    entity1_uniprot = relationship('PRtoUniProt', viewonly=True, uselist=False, primaryjoin='Cooccurrence.entity1_curie == PRtoUniProt.pr', lazy='joined')
    entity2_curie = Column(String(45))
    entity2_uniprot = relationship('PRtoUniProt', viewonly=True, uselist=False, primaryjoin='Cooccurrence.entity2_curie == PRtoUniProt.pr', lazy='joined')
    scores_list = relationship('CooccurrenceScores', primaryjoin='Cooccurrence.cooccurrence_id == CooccurrenceScores.cooccurrence_id', lazy='subquery')

    def __init__(self, cooccurrence_id, entity1_curie, entity2_curie):
        self.cooccurrence_id = cooccurrence_id
        self.entity1_curie = entity1_curie
        self.entity2_curie = entity2_curie


class CooccurrenceScores(Model):
    __tablename__ = 'cooccurrence_scores'
    cooccurrence_id = Column(String(27), ForeignKey('cooccurrence.cooccurrence_id'), ForeignKey('cooccurrence_publication.cooccurrence_id'), primary_key=True)
    level = Column(String(45), ForeignKey('cooccurrence_publication.level'), primary_key=True)
    publication_list = relationship('CooccurrencePublication', viewonly=True, uselist=True,
                                    primaryjoin='and_(CooccurrenceScores.cooccurrence_id == CooccurrencePublication.cooccurrence_id, CooccurrenceScores.level == CooccurrencePublication.level)', lazy='joined')
    concept1_count = Column(Integer)
    concept2_count = Column(Integer)
    pair_count = Column(Integer)
    ngd = Column(Float)
    pmi = Column(Float)
    pmi_norm = Column(Float)
    pmi_norm_max = Column(Float)
    mutual_dependence = Column(Float)
    lfmd = Column(Float)

    def __init__(self, cooccurrence_id, level, concept1_count, concept2_count, pair_count, ngd, pmi, pmi_norm, pmi_norm_max, mutual_dependence, lfmd):
        self.cooccurrence_id = cooccurrence_id
        self.level = level
        self.concept1_count = concept1_count
        self.concept2_count = concept2_count
        self.pair_count = pair_count
        self.ngd = ngd
        self.pmi = pmi
        self.pmi_norm = pmi_norm
        self.pmi_norm_max = pmi_norm_max
        self.mutual_dependence = mutual_dependence
        self.lfmd = lfmd


class CooccurrencePublication(Model):
    __tablename__ = 'cooccurrence_publication'
    cooccurrence_id = Column(String(27), ForeignKey('cooccurrence_scores.cooccurrence_id'), primary_key=True)
    level = Column(String(45), ForeignKey('cooccurrence_scores.level'), primary_key=True)
    document_id = Column(String(45), primary_key=True)

    def __init__(self, cooccurrence_id, level, document_id):
        self.cooccurrence_id = cooccurrence_id
        self.level = level
        self.document_id = document_id


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


# class Semmed(Model):
#     __tablename__ = 'semmed'
#     sid = Column('id', Integer, ForeignKey('tm_semmed.semmed_id'), primary_key=True)
#     pmid = Column(String(45))
#     sen_start_index = Column(Integer)
#     sentence = Column(String(2000))
#     sen_end_index = Column(Integer)
#     predicate = Column(String(45))
#     subject_cui = Column(String(45), ForeignKey('umls_to_obo.umls'))
#     subject_name = Column(String(250))
#     subject_curie = relationship('UmlsToObo', foreign_keys=subject_cui, lazy='joined')
#     object_cui = Column(String(45), ForeignKey('umls_to_obo.umls'))
#     object_name = Column(String(250))
#     object_curie = relationship('UmlsToObo', foreign_keys=object_cui, lazy='joined')
#     subject_start_index = Column(Integer)
#     subject_end_index = Column(Integer)
#     predicate_start_index = Column(Integer)
#     predicate_end_index = Column(Integer)
#
#     def __init__(self, pmid, sen_start_index, sentence, sen_end_index, predicate,
#                  subject_cui, subject_name, object_cui, object_name,
#                  subject_start_index, subject_end_index, predicate_start_index, predicate_end_index):
#         self.pmid = pmid
#         self.sen_start_index = sen_start_index
#         self.sentence = sentence
#         self.sen_end_index = sen_end_index
#         self.predicate = predicate
#         self.subject_cui = subject_cui
#         self.subject_name = subject_name
#         self.object_cui = object_cui
#         self.object_name = object_name
#         self.subject_start_index = subject_start_index
#         self.subject_end_index = subject_end_index
#         self.predicate_start_index = predicate_start_index
#         self.predicate_end_index = predicate_end_index
#
#
# class UmlsToObo(Model):
#     __tablename__ = 'umls_to_obo'
#     umls = Column(String(20), ForeignKey('semmed.subject_cui'), ForeignKey('semmed.object_cui'), primary_key=True)
#     obo = Column(String(20), primary_key=True)
#
#     def __init__(self, umls, obo):
#         self.umls = umls
#         self.obo = obo


class TmSemmed(Model):
    __tablename__ = 'tm_semmed'
    tm_id = Column(String(65), ForeignKey(Evidence.evidence_id), primary_key=True)
    # semmed_id = Column(Integer, ForeignKey(Semmed.sid), primary_key=True)
    semmed_id = Column(Integer, primary_key=True)
    # semmed = relationship('Semmed', foreign_keys=semmed_id, lazy='joined')

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
