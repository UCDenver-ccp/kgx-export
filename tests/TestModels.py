import os
import unittest
import hashlib
import random
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import models


class ModelTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None: # pragma: no cover
        if '\\tests' not in os.getcwd():
            os.chdir(f'{os.getcwd()}\\tests')

    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        session = sessionmaker()
        session.configure(bind=self.engine)
        self.session = session()
        models.Model.metadata.create_all(self.engine)
        self.populate_db()
        self.normalized_nodes = {
            "CHEBI:24433": {
                "id": {
                    "identifier": "CHEBI:24433",
                    "label": "group"
                },
                "type": [
                    "biolink:ChemicalEntity",
                ]
            },
            "PR:000000015": {
                "id": {
                    "identifier": "PR:000000015",
                    "label": "follistatin"
                },
                "type": [
                    "biolink:Protein",
                ]
            },
            "UniProtKB:P19883": {
                "id": {
                    "identifier": "UniProtKB:P19883",
                    "label": "FST_HUMAN Follistatin (sprot)"
                },
                "type": [
                    "biolink:Protein",
                ]
            }
        }


#region Query Tests

    def test_query_assertion(self):
        expected = [self.assertion, self.assertion2, self.assertion3,
                    self.assertion4, self.assertion5, self.assertion6]
        result = self.session.query(models.Assertion).all()
        self.assertCountEqual(result, expected)

    def test_query_evidence(self):
        expected = [self.evidence, self.evidence2, self.evidence3,
                    self.evidence4, self.evidence5, self.evidence6, self.evidence7]
        result = self.session.query(models.Evidence).all()
        self.assertCountEqual(result, expected)

    def test_query_entity(self):
        expected = [self.subject_entity, self.object_entity, self.subject_entity2, self.object_entity2,
                    self.subject_entity3, self.object_entity3, self.subject_entity4, self.object_entity4,
                    self.subject_entity5, self.object_entity5, self.subject_entity6, self.object_entity6,
                    self.subject_entity7, self.object_entity7]
        result = self.session.query(models.Entity).all()
        self.assertCountEqual(result, expected)

    def test_query_evaluation(self):
        expected = [self.evaluation, self.evaluation2, self.evaluation3,
                    self.evaluation4, self.evaluation5, self.evaluation6]
        result = self.session.query(models.Evaluation).all()
        self.assertCountEqual(result, expected)

    def test_query_evidence_score(self):
        expected = [self.evidence_score_1, self.evidence_score_2, self.evidence_score_3,
                    self.evidence_score_4, self.evidence_score_5, self.evidence_score_6,
                    self.evidence_score_7, self.evidence_score_8, self.evidence_score_9,
                    self.evidence_score_10, self.evidence_score_11, self.evidence_score_12,
                    self.evidence_score_13, self.evidence_score_14, self.evidence_score_15,
                    self.evidence_score_16, self.evidence_score_17, self.evidence_score_18,
                    self.evidence_score_19, self.evidence_score_20, self.evidence_score_21]
        result = self.session.query(models.EvidenceScore).all()
        self.assertCountEqual(result, expected)

    def test_query_pr_to_uniprot(self):
        expected = [self.pr_to_uniprot, self.pr_to_uniprot2]
        result = self.session.query(models.PRtoUniProt).all()
        self.assertCountEqual(result, expected)

    def test_query_cooccurrence_all(self):
        result = self.session.query(models.Cooccurrence).all()
        self.assertCountEqual(result, self.cooccurrence_records)

    def test_query_cooccurrence_scores_all(self):
        result = self.session.query(models.CooccurrenceScores).all()
        self.assertCountEqual(result, self.cooccurrence_scores_records)

    def test_query_cooccurrence_publication_all(self):
        result = self.session.query(models.CooccurrencePublication).all()
        self.assertCountEqual(result, self.cooccurrence_publication_records)

    def test_query_cooccurrence_publication_title(self):
        expected = [pub for pub in self.cooccurrence_publication_records if pub.level == 'title']
        result = self.session.query(models.CooccurrencePublication)\
            .where(models.CooccurrencePublication.level == 'title').all()
        self.assertCountEqual(result, expected)

    def test_query_cooccurrence_publication_abstract(self):
        expected = [pub for pub in self.cooccurrence_publication_records if pub.level == 'abstract']
        result = self.session.query(models.CooccurrencePublication)\
            .where(models.CooccurrencePublication.level == 'abstract').all()
        self.assertCountEqual(result, expected)

    def test_query_cooccurrence_publication_sentence(self):
        expected = [pub for pub in self.cooccurrence_publication_records if pub.level == 'sentence']
        result = self.session.query(models.CooccurrencePublication)\
            .where(models.CooccurrencePublication.level == 'sentence').all()
        self.assertCountEqual(result, expected)

    def test_query_cooccurrence_publication_document(self):
        expected = [pub for pub in self.cooccurrence_publication_records if pub.level == 'document']
        result = self.session.query(models.CooccurrencePublication)\
            .where(models.CooccurrencePublication.level == 'document').all()
        self.assertCountEqual(set(result), set(expected))

    def test_query_concept_idf(self):
        expected = [self.concept_idf]
        result = self.session.query(models.ConceptIDF).all()
        self.assertCountEqual(result, expected)

#endregion

#region Relationship Tests

    def test_relationship_assertion_to_evidence(self):
        expected = [self.evidence]
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        self.assertEqual(assertion_record.evidence_list, expected)

    def test_relationship_assertion_to_pr_to_uniprot(self):
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        self.assertIsNone(assertion_record.subject_uniprot)
        self.assertEqual(assertion_record.object_uniprot, self.pr_to_uniprot)

    def test_relationship_evidence_to_assertion(self):
        evidence_record = self.session.query(models.Evidence).where(models.Evidence.evidence_id == 'xyz').one()
        self.assertEqual(evidence_record.assertion, self.assertion)

    def test_relationship_evidence_to_entity(self):
        evidence_record = self.session.query(models.Evidence).where(models.Evidence.evidence_id == 'xyz').one()
        self.assertEqual(evidence_record.subject_entity, self.subject_entity)
        self.assertEqual(evidence_record.object_entity, self.object_entity)

    def test_relationship_evidence_to_evidence_score(self):
        expected = [self.evidence_score_1, self.evidence_score_2, self.evidence_score_3]
        evidence_record = self.session.query(models.Evidence).where(models.Evidence.evidence_id == 'xyz').one()
        self.assertEqual(evidence_record.evidence_scores, expected)

    def test_relationship_cooccurrence_score_to_cooccurrence_publication_title(self):
        cs_record = random.sample(self.cooccurrence_scores_records, 1)[0]
        expected = [pub for pub in self.cooccurrence_publication_records if pub.cooccurrence_id == cs_record.cooccurrence_id and pub.level == cs_record.level]
        cooccurrence_score = self.session.query(models.CooccurrenceScores)\
            .where(models.CooccurrenceScores.cooccurrence_id == cs_record.cooccurrence_id, models.CooccurrenceScores.level == cs_record.level).one()
        self.assertEqual(set(cooccurrence_score.publication_list), set(expected))

#endregion

#region Method Tests

    def test_method_assertion_get_predicate_scores(self):
        expected = {'biolink:entity_positively_regulates_entity': 0.999207900}
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        self.assertEqual(assertion_record.get_predicate_scores(), expected)

    def test_method_assertion_get_predicates(self):
        expected = {'biolink:entity_positively_regulates_entity'}
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        self.assertEqual(assertion_record.get_predicates(), expected)

    def test_method_assertion_get_aggregate_score(self):
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        self.assertEqual(assertion_record.get_aggregate_score('biolink:entity_positively_regulates_entity'), 0.999207900)

    def test_method_assertion_get_node_kgx(self):
        expected = [
            ['CHEBI:24433', 'group', 'biolink:ChemicalEntity'],
            ['PR:000000015', 'follistatin', 'biolink:Protein']
        ]
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        self.assertCountEqual(assertion_record.get_node_kgx(self.normalized_nodes), expected)

    def test_method_assertion_get_uniprot_node_kgx_positive(self):
        expected = [
            ['CHEBI:24433', 'group', 'biolink:ChemicalEntity'],
            ['UniProtKB:P19883', 'FST_HUMAN Follistatin (sprot)', 'biolink:Protein']
        ]
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        self.assertCountEqual(assertion_record.get_uniprot_node_kgx(self.normalized_nodes), expected)

    def test_method_assertion_get_uniprot_node_kgx_negative(self):
        expected = []
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'fghij').one()
        self.assertCountEqual(assertion_record.get_uniprot_node_kgx(self.normalized_nodes), expected)

    def test_method_assertion_get_edges_kgx(self):
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        edges = assertion_record.get_edges_kgx()
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0][1], "biolink:entity_positively_regulates_entity")

    def test_method_assertions_get_edge_kgx(self):
        expected_values = ['CHEBI:24433', 'biolink:entity_positively_regulates_entity', 'PR:000000015', 'abcde',
                           'biolink:ChemicalToGeneAssociation', 0.999207900, 'tmkp:xyz', 'PMID:32807176']
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        edge_kgx = assertion_record.get_edge_kgx("biolink:entity_positively_regulates_entity")
        json_attributes = json.loads(edge_kgx[-1])
        for value in expected_values:
            self.assertTrue(value in edge_kgx)

    def test_method_assertions_get_edge_kgx_limit(self):
        expected_values1 = ['CHEBI:24433', 'biolink:entity_positively_regulates_entity', 'PR:000000015', 'klmno',
                           'biolink:ChemicalToGeneAssociation', 0.9992, 'tmkp:nml|tmkp:kji',
                           'PMID:67133280|PMID:67193280']
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'klmno').one()
        edge_kgx1 = assertion_record.get_edge_kgx("biolink:entity_positively_regulates_entity")
        json_attributes1 = json.loads(edge_kgx1[-1])
        for value in expected_values1:
            self.assertTrue(value in edge_kgx1)
        self.assertEqual(len(self.get_attribute_list(json_attributes1, "biolink:supporting_study_result")), 2)
        expected_values2 = ['CHEBI:24433', 'biolink:entity_positively_regulates_entity', 'PR:000000015', 'klmno',
                           'biolink:ChemicalToGeneAssociation', 0.9992, 'tmkp:nml',
                           'PMID:67133280']
        edge_kgx2 = assertion_record.get_edge_kgx("biolink:entity_positively_regulates_entity", limit=1)
        json_attributes2 = json.loads(edge_kgx2[-1])
        for value in expected_values2:
            self.assertTrue(value in edge_kgx2)
        self.assertEqual(len(self.get_attribute_list(json_attributes2, "biolink:supporting_study_result")), 1)

    def test_method_assertions_get_edge_kgx_display_predicate_gain(self):
        expected_values = ['CHEBI:24433', 'biolink:contributes_to', 'PR:000000015', 'klmno',
                           'biolink:ChemicalToGeneAssociation', 0.9992, 'tmkp:tsr', 'PMID:67193280']
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'klmno').one()
        edge_kgx = assertion_record.get_edge_kgx("biolink:gain_of_function_contributes_to")
        for value in expected_values:
            self.assertTrue(value in edge_kgx)
        json_attributes = json.loads(edge_kgx[-1])
        self.assertIsNotNone(self.get_attribute_object(json_attributes, "biolink:sequence_variant_qualifier"))
        self.assertEqual(self.get_attribute_object(json_attributes, "biolink:sequence_variant_qualifier")["value"], "SO:0002053")

    def test_method_assertions_get_edge_kgx_display_predicate_loss(self):
        expected_values = ['CHEBI:24433', 'biolink:contributes_to', 'PR:000000015', 'zabcd',
                           'biolink:ChemicalToGeneAssociation', 0.9992, 'tmkp:hfe', 'PMID:66193280']
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'zabcd').one()
        edge_kgx = assertion_record.get_edge_kgx("biolink:loss_of_function_contributes_to")
        for value in expected_values:
            self.assertTrue(value in edge_kgx)
        json_attributes = json.loads(edge_kgx[-1])
        self.assertIsNotNone(self.get_attribute_object(json_attributes, "biolink:sequence_variant_qualifier"))
        self.assertEqual(self.get_attribute_object(json_attributes, "biolink:sequence_variant_qualifier")["value"], "SO:0002054")

    def test_method_assertions_get_other_edges_kgx(self):
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        edges = assertion_record.get_other_edges_kgx()
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0][1], "biolink:entity_positively_regulates_entity")
        self.assertEqual(edges[0][2], "UniProtKB:P19883")

    def test_method_assertions_get_other_edge_kgx_positive(self):
        expected_values = ['CHEBI:24433', 'biolink:entity_positively_regulates_entity', 'UniProtKB:P19883', 'abcde',
                           'biolink:ChemicalToGeneAssociation', 0.999207900, 'tmkp:xyz', 'PMID:32807176']
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        edge_kgx = assertion_record.get_other_edge_kgx("biolink:entity_positively_regulates_entity")
        json_attributes = json.loads(edge_kgx[-1])
        for value in expected_values:
            self.assertTrue(value in edge_kgx)

    def test_method_assertions_get_other_edge_kgx_limit(self):
        expected_values1 = ['CHEBI:24433', 'biolink:entity_positively_regulates_entity', 'UniProtKB:P19883', 'klmno',
                           'biolink:ChemicalToGeneAssociation', 0.9992, 'tmkp:nml|tmkp:kji',
                           'PMID:67133280|PMID:67193280']
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'klmno').one()
        edge_kgx1 = assertion_record.get_other_edge_kgx("biolink:entity_positively_regulates_entity")
        json_attributes1 = json.loads(edge_kgx1[-1])
        for value in expected_values1:
            self.assertTrue(value in edge_kgx1)
        self.assertEqual(len(self.get_attribute_list(json_attributes1, "biolink:supporting_study_result")), 2)
        expected_values2 = ['CHEBI:24433', 'biolink:entity_positively_regulates_entity', 'UniProtKB:P19883', 'klmno',
                           'biolink:ChemicalToGeneAssociation', 0.9992, 'tmkp:nml',
                           'PMID:67133280']
        edge_kgx2 = assertion_record.get_other_edge_kgx("biolink:entity_positively_regulates_entity", limit=1)
        json_attributes2 = json.loads(edge_kgx2[-1])
        for value in expected_values2:
            self.assertTrue(value in edge_kgx2)
        self.assertEqual(len(self.get_attribute_list(json_attributes2, "biolink:supporting_study_result")), 1)

    def test_method_assertions_get_other_edge_kgx_no_uniprot(self):
        expected = []
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'fghij').one()
        self.assertCountEqual(assertion_record.get_other_edge_kgx("biolink:entity_positively_regulates_entity"), expected)

    def test_method_assertions_get_other_edge_kgx_non_human_taxon(self):
        expected = []
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'pqrst').one()
        self.assertCountEqual(assertion_record.get_other_edge_kgx("biolink:entity_positively_regulates_entity"), expected)

    def test_method_assertions_get_other_edge_kgx_display_predicate_gain(self):
        expected_values = ['CHEBI:24433', 'biolink:contributes_to', 'UniProtKB:P19883', 'klmno',
                           'biolink:ChemicalToGeneAssociation', 0.9992, 'tmkp:tsr', 'PMID:67193280']
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'klmno').one()
        edge_kgx = assertion_record.get_other_edge_kgx("biolink:gain_of_function_contributes_to")
        for value in expected_values:
            self.assertTrue(value in edge_kgx)
        json_attributes = json.loads(edge_kgx[-1])
        self.assertIsNotNone(self.get_attribute_object(json_attributes, "biolink:sequence_variant_qualifier"))
        self.assertEqual(self.get_attribute_object(json_attributes, "biolink:sequence_variant_qualifier")["value"], "SO:0002053")

    def test_method_assertions_get_other_edge_kgx_display_predicate_loss(self):
        expected_values = ['CHEBI:24433', 'biolink:contributes_to', 'UniProtKB:P19883', 'zabcd',
                           'biolink:ChemicalToGeneAssociation', 0.9992, 'tmkp:hfe', 'PMID:66193280']
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'zabcd').one()
        edge_kgx = assertion_record.get_other_edge_kgx("biolink:loss_of_function_contributes_to")
        for value in expected_values:
            self.assertTrue(value in edge_kgx)
        json_attributes = json.loads(edge_kgx[-1])
        self.assertIsNotNone(self.get_attribute_object(json_attributes, "biolink:sequence_variant_qualifier"))
        self.assertEqual(self.get_attribute_object(json_attributes, "biolink:sequence_variant_qualifier")["value"], "SO:0002054")

    def test_method_assertions_get_json_attributes(self):
        assertion_record = self.session.query(models.Assertion).where(models.Assertion.assertion_id == 'abcde').one()
        edge_kgx = assertion_record.get_edge_kgx("biolink:entity_positively_regulates_entity")
        json_attributes = json.loads(edge_kgx[-1])
        self.assertIsNotNone(self.get_attribute_object(json_attributes, "biolink:original_knowledge_source"))
        study_result = self.get_attribute_list(json_attributes, "biolink:supporting_study_result")
        self.assertEqual(len(study_result), 1)

    def test_method_evidence_get_top_predicate(self):
        evidence_record = self.session.query(models.Evidence).where(models.Evidence.evidence_id == 'xyz').one()
        self.assertEqual(evidence_record.get_top_predicate(), 'biolink:entity_positively_regulates_entity')

    def test_method_evidence_get_predicates(self):
        expected = {'false', 'biolink:entity_negatively_regulates_entity', 'biolink:entity_positively_regulates_entity'}
        evidence_record = self.session.query(models.Evidence).where(models.Evidence.evidence_id == 'xyz').one()
        self.assertEqual(evidence_record.get_predicates(), expected)

    def test_method_evidence_get_score(self):
        evidence_record = self.session.query(models.Evidence).where(models.Evidence.evidence_id == 'xyz').one()
        self.assertEqual(evidence_record.get_score(), 0.999207900)
        self.assertEqual(evidence_record.get_score('false'), 0.000630744)
        self.assertEqual(evidence_record.get_score('biolink:entity_negatively_regulates_entity'), 0.000161453)
        self.assertEqual(evidence_record.get_score('biolink:entity_positively_regulates_entity'), 0.999207900)

    def test_method_evidence_get_json_attributes(self):
        evidence_record = self.session.query(models.Evidence).where(models.Evidence.evidence_id == 'xyz').one()
        json_attributes = evidence_record.get_json_attributes()
        self.assertEqual(json_attributes["attribute_type_id"], "biolink:supporting_study_result")
        self.assertEqual(len(json_attributes["attributes"]), 8)


#endregion

#region Helper Methods

    def generate_cooccurrence_records(self, curie_filename: str, record_count: int=10) -> list[models.Cooccurrence]:
        with open(curie_filename, 'r') as curie_file:
            curies = curie_file.read().splitlines()
        record_list = []
        id_hash = hashlib.sha1()
        for i in range(0, record_count):
            curie1, curie2 = random.sample(curies, 2)
            id_hash.update((curie1 + curie2).encode('utf-8'))
            record_list.append(models.Cooccurrence(id_hash.hexdigest()[:27], curie1, curie2))
        return record_list

    def generate_cooccurrence_scores_records(self, cooccurrence_list: list[models.Cooccurrence]) -> list[models.CooccurrenceScores]:
        record_list = []
        levels = ['abstract', 'document', 'sentence', 'title']
        for cooccurrence in cooccurrence_list:
            for level in levels:
                count1 = random.randint(0, 20000)
                count2 = random.randint(0, 20000)
                count3 = random.randint(0, 10000)
                score1 = random.random()
                score2 = random.random()
                score3 = random.random()
                score4 = random.random()
                score5 = random.random()
                score6 = random.random()
                record_list.append(models.CooccurrenceScores(cooccurrence.cooccurrence_id, level,
                                                             count1, count2, count3,
                                                             score1, score2, score3, score4, score5, score6))
        return record_list

    def generate_cooccurrence_publication_records(self, cooccurrence_list: list[models.Cooccurrence]) -> list[models.CooccurrencePublication]:
        record_list = []
        levels = ['abstract', 'document', 'sentence', 'title']
        for cooccurrence in cooccurrence_list:
            for level in levels:
                publication_count = random.randint(1, 10)
                for i in range(0, publication_count):
                    pmid = random.randint(10000000, 99999999)
                    record_list.append(
                        models.CooccurrencePublication(cooccurrence.cooccurrence_id, level, f"PMID:{pmid}"))
        return record_list

    def get_attribute_object(self, blob, atrribute_type_id) -> dict:
        for obj in blob:
            if obj['attribute_type_id'] == atrribute_type_id:
                return obj

    def get_attribute_list(self, blob, attribute_type_id) -> list:
        object_list = []
        for obj in blob:
            if obj['attribute_type_id'] == attribute_type_id:
                object_list.append(obj)
        return object_list


    def populate_db(self):
        self.pr_to_uniprot = models.PRtoUniProt('PR:000000015', 'UniProtKB:P19883', 'NCBITaxon:9606')
        self.session.add(self.pr_to_uniprot)
        self.pr_to_uniprot2 = models.PRtoUniProt('PR:000000016', 'UniProtKB:P19884', 'NCBITaxon:9605')
        self.session.add(self.pr_to_uniprot2)
        self.concept_idf = models.ConceptIDF('CHEBI:12345', 'abstract', 0.5)
        self.session.add(self.concept_idf)

        # basic assertion for "happy path" testing
        self.assertion = models.Assertion('abcde', 'CHEBI:24433', 'PR:000000015', 'biolink:ChemicalToGeneAssociation')
        self.session.add(self.assertion)
        self.evidence = models.Evidence('xyz', 'abcde', 'PMID:32807176', 'something', 'def', 'efd', 'title', 'article',
                                        2020)
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

        # assertion with untranslatable (non-uniprot) curie
        self.assertion2 = models.Assertion('fghij', 'CHEBI:00001', 'PR:000000001', 'biolink:ChemicalToGeneAssociation')
        self.session.add(self.assertion2)
        self.evidence2 = models.Evidence('zyx', 'fghij', 'PMID:67170823', 'something', 'ghi', 'ihg', 'title', 'article',
                                         2020)
        self.session.add(self.evidence2)
        self.subject_entity2 = models.Entity('ghi', '1|2', 'a')
        self.session.add(self.subject_entity2)
        self.object_entity2 = models.Entity('ihg', '2|4', 'be')
        self.session.add(self.object_entity2)
        self.evaluation2 = models.Evaluation('fghij', False, False, False, False, 7)
        self.session.add(self.evaluation2)
        self.evidence_score_4 = models.EvidenceScore('zyx', 'biolink:entity_negatively_regulates_entity', 0.161453)
        self.session.add(self.evidence_score_4)
        self.evidence_score_5 = models.EvidenceScore('zyx', 'biolink:entity_positively_regulates_entity', 0.9992)
        self.session.add(self.evidence_score_5)
        self.evidence_score_6 = models.EvidenceScore('zyx', 'false', 0.30744)
        self.session.add(self.evidence_score_6)

        # assertion with "contributes_to" predicate
        self.assertion3 = models.Assertion('klmno', 'CHEBI:24433', 'PR:000000015', 'biolink:ChemicalToGeneAssociation')
        self.session.add(self.assertion3)
        self.evidence3 = models.Evidence('tsr', 'klmno', 'PMID:67193280', 'something', 'jkl', 'lkj', 'abstract', 'article',
                                         2020)
        self.session.add(self.evidence3)
        self.subject_entity3 = models.Entity('jkl', '1|2', 'a')
        self.session.add(self.subject_entity3)
        self.object_entity3 = models.Entity('lkj', '2|4', 'be')
        self.session.add(self.object_entity3)
        self.evaluation3 = models.Evaluation('fghij', False, False, False, False, 7)
        self.session.add(self.evaluation3)
        self.evidence_score_7 = models.EvidenceScore('tsr', 'biolink:entity_negatively_regulates_entity', 0.161453)
        self.session.add(self.evidence_score_7)
        self.evidence_score_8 = models.EvidenceScore('tsr', 'biolink:gain_of_function_contributes_to', 0.9992)
        self.session.add(self.evidence_score_8)
        self.evidence_score_9 = models.EvidenceScore('tsr', 'false', 0.30744)
        self.session.add(self.evidence_score_9)

        # assertion with non-human taxon curie
        self.assertion4 = models.Assertion('pqrst', 'CHEBI:24433', 'PR:000000016', 'biolink:ChemicalToGeneAssociation')
        self.session.add(self.assertion4)
        self.evidence4 = models.Evidence('qpo', 'pqrst', 'PMID:67193280', 'something', 'pqr', 'rqp', 'abstract', 'article',
                                         2020)
        self.session.add(self.evidence4)
        self.subject_entity4 = models.Entity('pqr', '1|2', 'a')
        self.session.add(self.subject_entity4)
        self.object_entity4 = models.Entity('rqp', '2|4', 'be')
        self.session.add(self.object_entity4)
        self.evaluation4 = models.Evaluation('pqrst', False, False, False, False, 7)
        self.session.add(self.evaluation4)
        self.evidence_score_10 = models.EvidenceScore('qpo', 'biolink:entity_negatively_regulates_entity', 0.161453)
        self.session.add(self.evidence_score_10)
        self.evidence_score_11 = models.EvidenceScore('qpo', 'biolink:entity_positively_regulates_entity', 0.9992)
        self.session.add(self.evidence_score_11)
        self.evidence_score_12 = models.EvidenceScore('qpo', 'false', 0.30744)
        self.session.add(self.evidence_score_12)

        # assertion with multiple supporting studies
        self.assertion5 = models.Assertion('uvwxy', 'CHEBI:24433', 'PR:000000015', 'biolink:ChemicalToGeneAssociation')
        self.session.add(self.assertion5)
        self.evidence5 = models.Evidence('nml', 'klmno', 'PMID:67133280', 'something', 'stu', 'uts', 'abstract', 'article',
                                         2020)
        self.session.add(self.evidence5)
        self.evidence6 = models.Evidence('kji', 'klmno', 'PMID:67193280', 'something', 'vwx', 'xwv', 'abstract', 'article',
                                         2020)
        self.session.add(self.evidence6)
        self.subject_entity5 = models.Entity('stu', '1|2', 'a')
        self.session.add(self.subject_entity5)
        self.object_entity5 = models.Entity('uts', '2|4', 'be')
        self.session.add(self.object_entity5)
        self.subject_entity6 = models.Entity('vwx', '1|2', 'a')
        self.session.add(self.subject_entity6)
        self.object_entity6 = models.Entity('xwv', '2|4', 'be')
        self.session.add(self.object_entity6)
        self.evaluation5 = models.Evaluation('uvwxy', False, False, False, False, 1234)
        self.session.add(self.evaluation5)
        self.evidence_score_13 = models.EvidenceScore('nml', 'biolink:entity_positively_regulates_entity', 0.9992)
        self.session.add(self.evidence_score_13)
        self.evidence_score_14 = models.EvidenceScore('nml', 'biolink:entity_negatively_regulates_entity', 0.161453)
        self.session.add(self.evidence_score_14)
        self.evidence_score_15 = models.EvidenceScore('nml', 'false', 0.30744)
        self.session.add(self.evidence_score_15)
        self.evidence_score_16 = models.EvidenceScore('kji', 'biolink:entity_positively_regulates_entity', 0.9992)
        self.session.add(self.evidence_score_16)
        self.evidence_score_17 = models.EvidenceScore('kji', 'biolink:entity_negatively_regulates_entity', 0.161453)
        self.session.add(self.evidence_score_17)
        self.evidence_score_18 = models.EvidenceScore('kji', 'false', 0.30744)
        self.session.add(self.evidence_score_18)

        # assertion with the other "contributes_to" predicate
        self.assertion6 = models.Assertion('zabcd', 'CHEBI:24433', 'PR:000000015', 'biolink:ChemicalToGeneAssociation')
        self.session.add(self.assertion6)
        self.evidence7 = models.Evidence('hfe', 'zabcd', 'PMID:66193280', 'something', 'yza', 'azy', 'abstract', 'article',
                                         2020)
        self.session.add(self.evidence7)
        self.subject_entity7 = models.Entity('yza', '1|2', 'a')
        self.session.add(self.subject_entity7)
        self.object_entity7 = models.Entity('azy', '2|4', 'be')
        self.session.add(self.object_entity7)
        self.evaluation6 = models.Evaluation('zabcd', False, False, False, False, 7)
        self.session.add(self.evaluation6)
        self.evidence_score_19 = models.EvidenceScore('hfe', 'biolink:entity_negatively_regulates_entity', 0.161453)
        self.session.add(self.evidence_score_19)
        self.evidence_score_20 = models.EvidenceScore('hfe', 'biolink:loss_of_function_contributes_to', 0.9992)
        self.session.add(self.evidence_score_20)
        self.evidence_score_21 = models.EvidenceScore('hfe', 'false', 0.30744)
        self.session.add(self.evidence_score_21)

        self.cooccurrence_records = self.generate_cooccurrence_records('data/curies.txt', 100)
        self.cooccurrence_scores_records = self.generate_cooccurrence_scores_records(self.cooccurrence_records)
        self.cooccurrence_publication_records = self.generate_cooccurrence_publication_records(self.cooccurrence_records)
        self.session.add_all(self.cooccurrence_records)
        self.session.add_all(self.cooccurrence_scores_records)
        self.session.add_all(self.cooccurrence_publication_records)

        self.session.commit()

#endregion


    def tearDown(self) -> None:
        pass
