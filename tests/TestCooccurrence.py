import os.path
import unittest
import hashlib
import random
import json
from shutil import copyfile
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from typing import Iterator
from collections import namedtuple
import models
import cooccurrence
import services


class CooccurrenceTestCase(unittest.TestCase):
    ORIGINAL_KNOWLEDGE_SOURCE = "infores:text-mining-provider-cooccurrence"

    @classmethod
    def setUpClass(cls) -> None: # pragma: no cover
        if 'tests' not in os.getcwd():
            os.chdir(f'{os.getcwd()}/tests')

    def setUp(self) -> None:
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
            },
            "CHEBI:24444": {
                "id": {
                    "identifier": "CHEBI:24444",
                    "label": "test_chebi"
                }
            },
            "DRUGBANK:24444": {
                "id": {
                    "identifier": "DRUGBANK:24444",
                    "label": "test_drugbank"
                }
            }
        }

    def test_generate_metadata_compressed(self):
        copyfile('data/c_edges.tsv.gz', 'c_edges.tsv.gz')
        copyfile('data/c_nodes.tsv.gz', 'c_nodes.tsv.gz')
        result = cooccurrence.generate_metadata("c_nodes.tsv.gz", "c_edges.tsv.gz", compressed=True)
        self.assertIsInstance(result, dict)
        self.assertTrue("nodes" in result)
        self.assertIsInstance(result["nodes"], dict)
        self.assertTrue(len(result["nodes"]) > 0)
        self.assertTrue("edges" in result)
        self.assertIsInstance(result["edges"], list)
        self.assertTrue(len(result["edges"]) > 0)
        os.remove('c_edges.tsv.gz')
        os.remove('c_nodes.tsv.gz')

    def test_generate_metadata_uncompressed(self):
        pass

    def test_write_nodes_uniprot(self):
        curies = ['CHEBI:24433', 'PR:000000015', 'PR:000000016', 'UniProtKB:P19883']
        if not os.path.isdir('out'):
            os.mkdir('out')
        node_metadata = cooccurrence.write_nodes(self.session, 'out/test_nodes.tsv.gz', use_uniprot=True)
        self.assertTrue(os.path.isfile("out/test_nodes.tsv.gz"))
        self.assertTrue(len(node_metadata) > 0)
        os.remove("out/test_nodes.tsv.gz")

    def test_write_nodes_pr(self):
        curies = ['CHEBI:24433', 'PR:000000015', 'PR:000000016', 'PR:P19883']
        if not os.path.isdir('out'):
            os.mkdir('out')
        node_metadata = cooccurrence.write_nodes(self.session, 'out/test_nodes.tsv.gz', use_uniprot=False)
        self.assertTrue(os.path.isfile("out/test_nodes.tsv.gz"))
        self.assertTrue(len(node_metadata) > 0)
        os.remove("out/test_nodes.tsv.gz")

    def test_write_edges(self):
        pass

    def test_get_edge_kgx_uniprot(self):
        data_query_string = select(text(
            """
            IFNULL(u1.uniprot, entity1_curie) as curie1, u1.taxon as taxon1, 
            IFNULL(u2.uniprot, entity2_curie) as curie2, u2.taxon as taxon2, cs.*, 
            (
                SELECT GROUP_CONCAT(document_id, "|") 
                FROM cooccurrence_publication
                WHERE cooccurrence_id = c.cooccurrence_id AND level = cs.level
                GROUP BY cooccurrence_id, level
            ) as docstring
            FROM cooccurrence c 
            LEFT JOIN pr_to_uniprot u1 ON c.entity1_curie = u1.pr 
            LEFT JOIN pr_to_uniprot u2 ON c.entity2_curie = u2.pr
            LEFT JOIN cooccurrence_scores cs ON cs.cooccurrence_id = c.cooccurrence_id
            """
        ))
        for row in self.session.execute(data_query_string):
            result = cooccurrence.get_edge_kgx(row, use_uniprot=True)
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 7)
            self.assertFalse(result[0].startswith('PR:'))
            self.assertEqual(result[1], 'biolink:related_to')
            self.assertFalse(result[2].startswith('PR:'))
            self.assertEqual(result[4], 'biolink:Association')

    def test_get_edge_kgx_PR1(self):
        pr1 = {
            'curie1': 'PR:000000015', 'taxon1': None,
            'curie2': 'CHEBI:24433', 'taxon2': None,
            'docstring': 'PMID:12345678', 'cooccurrence_id': 'fake_id', 'level': 'abstract',
            'concept1_count': 5, 'concept2_count': 5, 'pair_count': 5,
            'ngd': 0.1, 'pmi': 0.2, 'pmi_norm': 0.3, 'pmi_norm_max': 0.4, 'mutual_dependence': 0.5, 'lfmd': 0.6
        }
        pr1_pr_result = cooccurrence.get_edge_kgx(pr1, use_uniprot=False)
        self.assertIsInstance(pr1_pr_result, list)
        self.assertEqual(len(pr1_pr_result), 7)
        pr1_uniprot_result = cooccurrence.get_edge_kgx(pr1, use_uniprot=True)
        self.assertIsInstance(pr1_uniprot_result, list)
        self.assertEqual(len(pr1_uniprot_result), 0)

    def test_get_edge_kgx_PR2(self):
        pr2 = {
            'curie1': 'CHEBI:24433', 'taxon1': None,
            'curie2': 'PR:000000015', 'taxon2': None,
            'docstring': 'PMID:12345678', 'cooccurrence_id': 'fake_id', 'level': 'abstract',
            'concept1_count': 5, 'concept2_count': 5, 'pair_count': 5,
            'ngd': 0.1, 'pmi': 0.2, 'pmi_norm': 0.3, 'pmi_norm_max': 0.4, 'mutual_dependence': 0.5, 'lfmd': 0.6
        }
        pr2_pr_result = cooccurrence.get_edge_kgx(pr2, use_uniprot=False)
        self.assertIsInstance(pr2_pr_result, list)
        self.assertEqual(len(pr2_pr_result), 7)
        pr2_uniprot_result = cooccurrence.get_edge_kgx(pr2, use_uniprot=True)
        self.assertIsInstance(pr2_uniprot_result, list)
        self.assertEqual(len(pr2_uniprot_result), 0)

    def test_get_edge_kgx_Taxon1(self):
        taxon1 = {
            'curie1': 'UniProtKB:P19883', 'taxon1': 'NCBITaxon:9605',
            'curie2': 'CHEBI:24433', 'taxon2': None,
            'docstring': 'PMID:12345678', 'cooccurrence_id': 'fake_id', 'level': 'abstract',
            'concept1_count': 5, 'concept2_count': 5, 'pair_count': 5,
            'ngd': 0.1, 'pmi': 0.2, 'pmi_norm': 0.3, 'pmi_norm_max': 0.4, 'mutual_dependence': 0.5, 'lfmd': 0.6
        }
        taxon1_pr_result = cooccurrence.get_edge_kgx(taxon1, use_uniprot=False)
        self.assertIsInstance(taxon1_pr_result, list)
        self.assertEqual(len(taxon1_pr_result), 7)
        taxon1_uniprot_result = cooccurrence.get_edge_kgx(taxon1, use_uniprot=True)
        self.assertIsInstance(taxon1_uniprot_result, list)
        self.assertEqual(len(taxon1_uniprot_result), 0)

    def test_get_edge_kgx_Taxon2(self):
        taxon2 = {
            'curie1': 'CHEBI:24433', 'taxon1': None,
            'curie2': 'UniProtKB:P19883', 'taxon2': 'NCBITaxon:9605',
            'docstring': 'PMID:12345678', 'cooccurrence_id': 'fake_id', 'level': 'abstract',
            'concept1_count': 5, 'concept2_count': 5, 'pair_count': 5,
            'ngd': 0.1, 'pmi': 0.2, 'pmi_norm': 0.3, 'pmi_norm_max': 0.4, 'mutual_dependence': 0.5, 'lfmd': 0.6
        }

        taxon2_pr_result = cooccurrence.get_edge_kgx(taxon2, use_uniprot=False)
        self.assertIsInstance(taxon2_pr_result, list)
        self.assertEqual(len(taxon2_pr_result), 7)
        taxon2_uniprot_result = cooccurrence.get_edge_kgx(taxon2, use_uniprot=True)
        self.assertIsInstance(taxon2_uniprot_result, list)
        self.assertEqual(len(taxon2_uniprot_result), 0)

    def test_get_json_attributes(self):
        row = {
            'curie1': 'UniProtKB:P19883', 'taxon1': cooccurrence.HUMAN_TAXON,
            'curie2': 'CHEBI:24433', 'taxon2': None,
            'docstring': 'PMID:12345678', 'cooccurrence_id': 'fake_id', 'level': 'abstract',
            'concept1_count': 5, 'concept2_count': 5, 'pair_count': 5,
            'ngd': 0.1, 'pmi': 0.2, 'pmi_norm': 0.3, 'pmi_norm_max': 0.4, 'mutual_dependence': 0.5, 'lfmd': 0.6
        }
        json_attributes = cooccurrence.get_json_attributes(row)
        self.assertIsNotNone(self.get_attribute_object(json_attributes, "biolink:original_knowledge_source"))
        self.assertIsNotNone(self.get_attribute_object(json_attributes, "biolink:supporting_data_source"))

    def test_scores_to_json(self):
        pass

    def test_create_kge_tarball(self):
        pass

#region Helper Methods

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

    def populate_db(self):
        self.cooccurrence_records = self.generate_cooccurrence_records('data/curies.txt', 100)
        self.cooccurrence_scores_records = self.generate_cooccurrence_scores_records(self.cooccurrence_records)
        self.cooccurrence_publication_records = self.generate_cooccurrence_publication_records(
            self.cooccurrence_records)
        self.session.add_all(self.cooccurrence_records)
        self.session.add_all(self.cooccurrence_scores_records)
        self.session.add_all(self.cooccurrence_publication_records)

        self.session.commit()

# endregion

    def tearDown(self) -> None:
        pass
