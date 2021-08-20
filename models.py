import os
import json
from sqlalchemy import Column, String, Integer, Boolean, Float, ForeignKey, UniqueConstraint, create_engine, engine
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from math import fsum

Model = declarative_base(name='Model')


class Assertion(Model):
    __tablename__ = 'assertion'
    id = Column(Integer, primary_key=True)
    assertion_id = Column(String, primary_key=True)
    subject_curie = Column(String)
    object_curie = Column(String)
    association_curie = Column(String)
    evidence_list = relationship('Evidence', back_populates='assertion')

    def get_aggregate_score(self):
        if len(self.evidence_list) == 0:
            return 0.0
        return fsum([x.get_aggregate_score() for x in self.evidence_list]) / float(len(self.evidence_list))

    def get_json_attributes(self):
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
                "value": "infores:pubmed", # this will need to come from the db, eventually
                "value_type_id": "biolink:InformationResource",
                "attribute_source": "infores:text-mining-provider-targeted"
            },
            {
                "attribute_type_id": "biolink:tmkp_confidence_score",
                "value": self.get_aggregate_score(),
                "value_type_id": "biolink:ConfidenceLevel",
                "description": "An aggregate confidence score that combines evidence from all sentences that support the edge",
                "attribute_source": "infores:text-mining-provider-targeted"
            }
        ]
        for study in self.evidence_list:
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
    assertion_id = Column(String)
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
    subject_entity = relationship('Entity', foreign_keys=subject_entity_id)
    object_entity_id = Column(String, ForeignKey('entity.entity_id'))
    object_entity = relationship('Entity', foreign_keys=object_entity_id)
    document_zone = Column(String)
    document_publication_type = Column(String)
    document_year_published = Column(Integer)

    def get_aggregate_score(self):
        if len(self.evidence_scores) == 0:
            return 0.0
        return fsum([es.score for es in self.evidence_scores]) / float(len(self.evidence_scores))

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
                    "value": self.get_aggregate_score(),
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
    evidence = relationship('Evidence', backref='evidence_scores')
    predicate_curie = Column(String, primary_key=True)
    score = Column(Float)
    UniqueConstraint('evidence_id', 'predicate_curie', name='evidence+predicate')


secret_password = os.getenv('MYSQL_DATABASE_PASSWORD', None)
assert secret_password
url = engine.url.URL.create(
    drivername="mysql+pymysql",
    username='edgar',
    password=secret_password,
    database='text_mined_assertions',
    host='localhost',
    port=3306
    # query={
    #     "unix_socket": "/cloudsql/lithe-vault-265816:us-central1:text-mined-assertions-stage"
    # }
)
engine = create_engine(url, echo=True, future=True)
session = sessionmaker()
session.configure(bind=engine)
