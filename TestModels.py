import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import models


class ModelTestCase(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        session = sessionmaker()
        session.configure(bind=self.engine)
        self.session = session()
        models.Model.metadata.create_all(self.engine)
        self.populate_db()

    def populate_db(self):
        self.assertion = models.Assertion('abcde', 'CHEBI:24433', 'PR:000000015', 'biolink:ChemicalToGeneAssociation')
        self.assertion.id = 1
        self.session.add(self.assertion)
        self.evidence = models.Evidence('xyz', 'abcde', 'PMID:32807176', 'something', 'def', 'efd', 'title', 'article', 2020)
        self.session.add(self.evidence)
        self.subject_entity = models.Entity('def', '1|2', 'a')
        self.session.add(self.subject_entity)
        self.object_entity = models.Entity('efd', '2|4', 'be')
        self.session.add(self.object_entity)
        self.evaluation = models.Evaluation('abcde', False, False, False, False, 1234)
        self.session.add(self.evaluation)
        self.evidence_score_1 = models.EvidenceScore('xyz', 'biolink:entity_negatively_regulates_entity', 0.000161453)
        self.session.add(self.evidence_score_1)
        self.evidence_score_2 = models.EvidenceScore('xyz', 'biolink:entity_positively_regulates_entity', 0.999207900)
        self.session.add(self.evidence_score_2)
        self.evidence_score_3 = models.EvidenceScore('xyz', 'false', 0.000630744)
        self.session.add(self.evidence_score_3)
        self.pr_to_uniprot = models.PRtoUniProt('PR:000000015', 'UniProtKB:P19883', 'NCBITaxon:9606')
        self.session.add(self.pr_to_uniprot)
        self.cooccurrence = models.Cooccurrence('ghi', 'CHEBI:00001', 'PR:000000001')
        self.session.add(self.cooccurrence)
        self.cooccurrence_scores_title = models.CooccurrenceScores('ghi', 'title', 123, 456, 78, 0.79529121, 2.4687596, 0.14797458, 0.78078191, -14.2326750, -30.9341095)
        self.session.add(self.cooccurrence_scores_title)
        self.cooccurrence_scores_abstract = models.CooccurrenceScores('ghi', 'abstract', 135, 791, 15, 0.91823842, -0.0071880, -0.00044927, -0.00153756, -16.0154754, -32.0237628)
        self.session.add(self.cooccurrence_scores_abstract)
        self.cooccurrence_scores_sentence = models.CooccurrenceScores('ghi', 'sentence', 246, 802, 178, 0.97411809, -0.7722175, -0.04826550, -0.11321754, -16.7805048, -32.7887922)
        self.session.add(self.cooccurrence_scores_sentence)
        self.cooccurrence_scores_document = models.CooccurrenceScores('ghi', 'document', 741, 852, 700, 0.85643479, -0.6615288, 0.04134718, 0.11027196, -15.3467586, -31.3550460)
        self.session.add(self.cooccurrence_scores_document)
        self.coocurrence_publication_title_1 = models.CooccurrencePublication('ghi', 'title', 'PMID:34570630')
        self.session.add(self.coocurrence_publication_title_1)
        self.coocurrence_publication_title_2 = models.CooccurrencePublication('ghi', 'title', 'PMID:34566824')
        self.session.add(self.coocurrence_publication_title_2)
        self.coocurrence_publication_title_3 = models.CooccurrencePublication('ghi', 'title', 'PMID:34557406')
        self.session.add(self.coocurrence_publication_title_3)
        self.coocurrence_publication_title_4 = models.CooccurrencePublication('ghi', 'title', 'PMID:34557200')
        self.session.add(self.coocurrence_publication_title_4)
        self.coocurrence_publication_abstract_1 = models.CooccurrencePublication('ghi', 'abstract', 'PMID:34556781')
        self.session.add(self.coocurrence_publication_abstract_1)
        self.coocurrence_publication_abstract_2 = models.CooccurrencePublication('ghi', 'abstract', 'PMID:34556089')
        self.session.add(self.coocurrence_publication_abstract_2)
        self.coocurrence_publication_abstract_3 = models.CooccurrencePublication('ghi', 'abstract', 'PMID:34555076')
        self.session.add(self.coocurrence_publication_abstract_3)
        self.coocurrence_publication_abstract_4 = models.CooccurrencePublication('ghi', 'abstract', 'PMID:34554099')
        self.session.add(self.coocurrence_publication_abstract_4)
        self.coocurrence_publication_sentence_1 = models.CooccurrencePublication('ghi', 'sentence', 'PMID:34553167')
        self.session.add(self.coocurrence_publication_sentence_1)
        self.coocurrence_publication_sentence_2 = models.CooccurrencePublication('ghi', 'sentence', 'PMID:34552921')
        self.session.add(self.coocurrence_publication_sentence_2)
        self.coocurrence_publication_sentence_3 = models.CooccurrencePublication('ghi', 'sentence', 'PMID:34552519')
        self.session.add(self.coocurrence_publication_sentence_3)
        self.coocurrence_publication_sentence_4 = models.CooccurrencePublication('ghi', 'sentence', 'PMID:34550967')
        self.session.add(self.coocurrence_publication_sentence_4)
        self.coocurrence_publication_document_1 = models.CooccurrencePublication('ghi', 'document', 'PMID:34550383')
        self.session.add(self.coocurrence_publication_document_1)
        self.coocurrence_publication_document_2 = models.CooccurrencePublication('ghi', 'document', 'PMID:34548706')
        self.session.add(self.coocurrence_publication_document_2)
        self.coocurrence_publication_document_3 = models.CooccurrencePublication('ghi', 'document', 'PMID:34547029')
        self.session.add(self.coocurrence_publication_document_3)
        self.coocurrence_publication_document_4 = models.CooccurrencePublication('ghi', 'document', 'PMID:34547014')
        self.session.add(self.coocurrence_publication_document_4)
        self.concept_idf = models.ConceptIDF('CHEBI:12345', 'abstract', 0.5)
        self.session.add(self.concept_idf)
        self.session.commit()

    # def tearDown(self):
    #     models.Model.metadata.drop_all(self.engine)

# Query Tests

    def test_query_assertion(self):
        expected = [self.assertion]
        result = self.session.query(models.Assertion).all()
        self.assertEqual(result, expected)

    def test_query_evidence(self):
        expected = [self.evidence]
        result = self.session.query(models.Evidence).all()
        self.assertEqual(result, expected)

    def test_query_entity(self):
        expected = [self.subject_entity, self.object_entity]
        result = self.session.query(models.Entity).all()
        self.assertEqual(result, expected)

    def test_query_evaluation(self):
        expected = [self.evaluation]
        result = self.session.query(models.Evaluation).all()
        self.assertEqual(result, expected)

    def test_query_evidence_score(self):
        expected = [self.evidence_score_1, self.evidence_score_2, self.evidence_score_3]
        result = self.session.query(models.EvidenceScore).all()
        self.assertEqual(result, expected)

    def test_query_pr_to_uniprot(self):
        expected = [self.pr_to_uniprot]
        result = self.session.query(models.PRtoUniProt).all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence(self):
        expected = [self.cooccurrence]
        result = self.session.query(models.Cooccurrence).all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence_scores(self):
        expected = [self.cooccurrence_scores_title, self.cooccurrence_scores_abstract, self.cooccurrence_scores_sentence, self.cooccurrence_scores_document]
        result = self.session.query(models.CooccurrenceScores).all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence_publication_all(self):
        expected = [
            self.coocurrence_publication_title_1,
            self.coocurrence_publication_title_2,
            self.coocurrence_publication_title_3,
            self.coocurrence_publication_title_4,
            self.coocurrence_publication_abstract_1,
            self.coocurrence_publication_abstract_2,
            self.coocurrence_publication_abstract_3,
            self.coocurrence_publication_abstract_4,
            self.coocurrence_publication_sentence_1,
            self.coocurrence_publication_sentence_2,
            self.coocurrence_publication_sentence_3,
            self.coocurrence_publication_sentence_4,
            self.coocurrence_publication_document_1,
            self.coocurrence_publication_document_2,
            self.coocurrence_publication_document_3,
            self.coocurrence_publication_document_4
        ]
        result = self.session.query(models.CooccurrencePublication).all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence_publication_all(self):
        expected = [
            self.coocurrence_publication_title_1,
            self.coocurrence_publication_title_2,
            self.coocurrence_publication_title_3,
            self.coocurrence_publication_title_4,
            self.coocurrence_publication_abstract_1,
            self.coocurrence_publication_abstract_2,
            self.coocurrence_publication_abstract_3,
            self.coocurrence_publication_abstract_4,
            self.coocurrence_publication_sentence_1,
            self.coocurrence_publication_sentence_2,
            self.coocurrence_publication_sentence_3,
            self.coocurrence_publication_sentence_4,
            self.coocurrence_publication_document_1,
            self.coocurrence_publication_document_2,
            self.coocurrence_publication_document_3,
            self.coocurrence_publication_document_4
        ]
        result = self.session.query(models.CooccurrencePublication).all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence_publication_all(self):
        expected = [
            self.coocurrence_publication_title_1,
            self.coocurrence_publication_title_2,
            self.coocurrence_publication_title_3,
            self.coocurrence_publication_title_4,
            self.coocurrence_publication_abstract_1,
            self.coocurrence_publication_abstract_2,
            self.coocurrence_publication_abstract_3,
            self.coocurrence_publication_abstract_4,
            self.coocurrence_publication_sentence_1,
            self.coocurrence_publication_sentence_2,
            self.coocurrence_publication_sentence_3,
            self.coocurrence_publication_sentence_4,
            self.coocurrence_publication_document_1,
            self.coocurrence_publication_document_2,
            self.coocurrence_publication_document_3,
            self.coocurrence_publication_document_4
        ]
        result = self.session.query(models.CooccurrencePublication).all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence_publication_title(self):
        expected = [
            self.coocurrence_publication_title_1,
            self.coocurrence_publication_title_2,
            self.coocurrence_publication_title_3,
            self.coocurrence_publication_title_4
        ]
        result = self.session.query(models.CooccurrencePublication)\
            .where(models.CooccurrencePublication.level == 'title').all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence_publication_abstract(self):
        expected = [
            self.coocurrence_publication_abstract_1,
            self.coocurrence_publication_abstract_2,
            self.coocurrence_publication_abstract_3,
            self.coocurrence_publication_abstract_4,
        ]
        result = self.session.query(models.CooccurrencePublication)\
            .where(models.CooccurrencePublication.level == 'abstract').all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence_publication_sentence(self):
        expected = [
            self.coocurrence_publication_sentence_1,
            self.coocurrence_publication_sentence_2,
            self.coocurrence_publication_sentence_3,
            self.coocurrence_publication_sentence_4,
        ]
        result = self.session.query(models.CooccurrencePublication)\
            .where(models.CooccurrencePublication.level == 'sentence').all()
        self.assertEqual(result, expected)

    def test_query_cooccurrence_publication_document(self):
        expected = [
            self.coocurrence_publication_document_1,
            self.coocurrence_publication_document_2,
            self.coocurrence_publication_document_3,
            self.coocurrence_publication_document_4
        ]
        result = self.session.query(models.CooccurrencePublication)\
            .where(models.CooccurrencePublication.level == 'document').all()
        self.assertEqual(set(result), set(expected))

    def test_query_concept_idf(self):
        expected = [self.concept_idf]
        result = self.session.query(models.ConceptIDF).all()
        self.assertEqual(result, expected)

# Relationship Tests

    def test_relationship_assertion_to_evidence(self):
        expected = [self.evidence]
        assertion_record = self.session.query(models.Assertion).one()
        self.assertEqual(assertion_record.evidence_list, expected)

    def test_relationship_assertion_to_pr_to_uniprot(self):
        assertion_record = self.session.query(models.Assertion).one()
        self.assertIsNone(assertion_record.subject_uniprot)
        self.assertEqual(assertion_record.object_uniprot, self.pr_to_uniprot)

    def test_relationship_evidence_to_assertion(self):
        evidence_record = self.session.query(models.Evidence).one()
        self.assertEqual(evidence_record.assertion, self.assertion)

    def test_relationship_evidence_to_entity(self):
        evidence_record = self.session.query(models.Evidence).one()
        self.assertEqual(evidence_record.subject_entity, self.subject_entity)
        self.assertEqual(evidence_record.object_entity, self.object_entity)

    def test_relationship_evidence_to_evidence_score(self):
        expected = [self.evidence_score_1, self.evidence_score_2, self.evidence_score_3]
        evidence_record = self.session.query(models.Evidence).one()
        self.assertEqual(evidence_record.evidence_scores, expected)

    def test_relationship_cooccurrence_score_to_cooccurrence_publication_title(self):
        expected = [
            self.coocurrence_publication_title_1,
            self.coocurrence_publication_title_2,
            self.coocurrence_publication_title_3,
            self.coocurrence_publication_title_4
        ]
        cooccurrence_score = self.session.query(models.CooccurrenceScores)\
            .where(models.CooccurrenceScores.level == 'title').one()
        self.assertEqual(set(cooccurrence_score.publication_list), set(expected))

# Method Tests

    def test_method_assertion_get_predicate_scores(self):
        expected = {'biolink:entity_positively_regulates_entity': 0.999207900}
        assertion_record = self.session.query(models.Assertion).one()
        self.assertEqual(assertion_record.get_predicate_scores(), expected)

    def test_method_assertion_get_predicates(self):
        expected = {'biolink:entity_positively_regulates_entity'}
        assertion_record = self.session.query(models.Assertion).one()
        self.assertEqual(assertion_record.get_predicates(), expected)

    def test_method_assertion_get_aggregate_score(self):
        assertion_record = self.session.query(models.Assertion).one()
        self.assertEqual(assertion_record.get_aggregate_score('biolink:entity_positively_regulates_entity'), 0.999207900)

    def test_method_evidence_get_top_predicate(self):
        evidence_record = self.session.query(models.Evidence).one()
        self.assertEqual(evidence_record.get_top_predicate(), 'biolink:entity_positively_regulates_entity')

    def test_method_evidence_get_predicates(self):
        expected = {'false', 'biolink:entity_negatively_regulates_entity', 'biolink:entity_positively_regulates_entity'}
        evidence_record = self.session.query(models.Evidence).one()
        self.assertEqual(evidence_record.get_predicates(), expected)

    def test_method_evidence_get_score(self):
        evidence_record = self.session.query(models.Evidence).one()
        self.assertEqual(evidence_record.get_score(), 0.999207900)
        self.assertEqual(evidence_record.get_score('false'), 0.000630744)
        self.assertEqual(evidence_record.get_score('biolink:entity_negatively_regulates_entity'), 0.000161453)
        self.assertEqual(evidence_record.get_score('biolink:entity_positively_regulates_entity'), 0.999207900)
