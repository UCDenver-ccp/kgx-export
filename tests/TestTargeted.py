import os.path
import unittest
import hashlib
import random
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Iterator
import models
import targeted


class TargetedTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None: # pragma: no cover
        if '\\tests' not in os.getcwd():
            os.chdir(f'{os.getcwd()}\\tests')

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

#region DB-independent Tests

    def test_update_node_metadata_new(self):
        expected = {
            "biolink:ChemicalEntity": {
                "id_prefixes": ["CHEBI"],
                "count": 1,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 1
                    }
                }
            }
        }
        node = ['CHEBI:24433', 'group', 'biolink:ChemicalEntity']
        initial = {}
        result = targeted.update_node_metadata(node, initial)
        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_update_node_metadata_existing(self):
        initial = {
            "biolink:ChemicalEntity": {
                "id_prefixes": ["CHEBI"],
                "count": 1,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 1
                    }
                }
            }
        }
        expected = {
            "biolink:ChemicalEntity": {
                "id_prefixes": ["CHEBI"],
                "count": 2,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 2
                    }
                }
            }
        }
        node = ['CHEBI:24433', 'group', 'biolink:ChemicalEntity']
        result = targeted.update_node_metadata(node, initial)
        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_update_edge_metadata_new_triple(self):
        expected = {
            "biolink:Protein|biolink:entity_negatively_regulates_entity|biolink:ChemicalEntity": {
                "subject": "biolink:ChemicalEntity",
                "predicate": "biolink:entity_negatively_regulates_entity",
                "object": "biolink:Protein",
                "relations": ["biolink:ChemicalToGeneAssociation"],
                "count": 1,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 1
                    }
                }
            }
        }
        initial = {}
        edge = ["PR:000000015", "biolink:entity_negatively_regulates_entity", "CHEBI:24433", 'fake_id', "biolink:ChemicalToGeneAssociation"]
        result = targeted.update_edge_metadata(edge, initial, self.normalized_nodes)
        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_update_edge_metadata_new_relation(self):
        initial = {
            "biolink:Protein|biolink:entity_negatively_regulates_entity|biolink:ChemicalEntity": {
                "subject": "biolink:ChemicalEntity",
                "predicate": "biolink:entity_negatively_regulates_entity",
                "object": "biolink:Protein",
                "relations": ["biolink:ChemicalToGeneAssociation"],
                "count": 1,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 1
                    }
                }
            }
        }
        expected = {
            "biolink:Protein|biolink:entity_negatively_regulates_entity|biolink:ChemicalEntity": {
                "subject": "biolink:ChemicalEntity",
                "predicate": "biolink:entity_negatively_regulates_entity",
                "object": "biolink:Protein",
                "relations": ["biolink:ChemicalToGeneAssociation", "biolink:TestAssociation"],
                "count": 2,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 2
                    }
                }
            }
        }
        edge = ["PR:000000015", "biolink:entity_negatively_regulates_entity", "CHEBI:24433", 'fake_id', "biolink:TestAssociation"]
        result = targeted.update_edge_metadata(edge, initial, self.normalized_nodes)
        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_update_edge_metadata_existing(self):
        initial = {
            "biolink:Protein|biolink:entity_negatively_regulates_entity|biolink:ChemicalEntity": {
                "subject": "biolink:ChemicalEntity",
                "predicate": "biolink:entity_negatively_regulates_entity",
                "object": "biolink:Protein",
                "relations": ["biolink:ChemicalToGeneAssociation"],
                "count": 1,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 1
                    }
                }
            }
        }
        expected = {
            "biolink:Protein|biolink:entity_negatively_regulates_entity|biolink:ChemicalEntity": {
                "subject": "biolink:ChemicalEntity",
                "predicate": "biolink:entity_negatively_regulates_entity",
                "object": "biolink:Protein",
                "relations": ["biolink:ChemicalToGeneAssociation"],
                "count": 2,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 2
                    }
                }
            }
        }
        edge = ["PR:000000015", "biolink:entity_negatively_regulates_entity", "CHEBI:24433", 'fake_id', "biolink:ChemicalToGeneAssociation"]
        result = targeted.update_edge_metadata(edge, initial, self.normalized_nodes)
        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_get_category_default_category(self):
        self.assertEqual(targeted.get_category("CHEBI:24444", self.normalized_nodes), "biolink:NamedThing")

    def test_get_category_default_category_drugbank(self):
        self.assertEqual(targeted.get_category("DRUGBANK:24444", self.normalized_nodes), "biolink:SmallMolecule")

    def test_get_category_found(self):
        self.assertEqual(targeted.get_category("CHEBI:24433", self.normalized_nodes), "biolink:ChemicalEntity")

    def test_is_normal_true(self):
        self.assertTrue(targeted.is_normal("PR:000000015", self.normalized_nodes))

    def test_is_normal_false(self):
        self.assertFalse(targeted.is_normal("PR:000000016", self.normalized_nodes))

    def test_get_kgx_nodes_type_check(self):
        curies = ['CHEBI:24433', 'PR:000000015', 'UniProtKB:P19883']
        results = targeted.get_kgx_nodes(curies, self.normalized_nodes)
        self.assertIsInstance(results, Iterator)
        for item in results:
            self.assertIsInstance(item, list)
            self.assertEqual(len(item), 3)

    def test_get_kgx_nodes_default_category(self):
        result_iterator = targeted.get_kgx_nodes(['CHEBI:24444'], self.normalized_nodes)
        result = next(result_iterator)
        self.assertEqual(result, ['CHEBI:24444', 'test_chebi', 'biolink:NamedThing'])

    def test_get_kgx_nodes_default_category_drugbank(self):
        result_iterator = targeted.get_kgx_nodes(['DRUGBANK:24444'], self.normalized_nodes)
        result = next(result_iterator)
        self.assertEqual(result, ['DRUGBANK:24444', 'test_drugbank', 'biolink:SmallMolecule'])

    def test_get_kgx_nodes_not_normal(self):
        result_iterator = targeted.get_kgx_nodes(['PR:000000016'], self.normalized_nodes)
        result = next(result_iterator)
        self.assertEqual(result, [])

    def test_get_kgx_nodes_normal(self):
        result_iterator = targeted.get_kgx_nodes(['CHEBI:24433'], self.normalized_nodes)
        result = next(result_iterator)
        self.assertEqual(result, ['CHEBI:24433', 'group', 'biolink:ChemicalEntity'])

#endregion

#region DB-dependent Tests

    def test_get_node_data_pr(self):
        subject_curies = set(assertion.subject_curie for assertion in self.assertions)
        object_curies = set(assertion.object_curie for assertion in self.assertions)
        expected_curies = list(set.union(subject_curies, object_curies))
        (curies, normalize_dict) = targeted.get_node_data(self.session, use_uniprot=False)
        self.assertCountEqual(curies, expected_curies)

    def test_get_node_data_uniprot(self):
        subject_curies = set(assertion.subject_curie for assertion in self.assertions)
        object_curies = set(assertion.object_curie for assertion in self.assertions)
        all_curies = list(set.union(subject_curies, object_curies))
        expected_curies = [curie for curie in all_curies if not curie.startswith('PR:')]
        (curies, normalize_dict) = targeted.get_node_data(self.session, use_uniprot=True)
        self.assertCountEqual(curies, expected_curies)

    def test_write_nodes(self):
        curies = ['CHEBI:24433', 'PR:000000015', 'PR:000000016', 'UniProtKB:P19883']
        node_metadata = targeted.write_nodes(curies, self.normalized_nodes, 'out/test_nodes.tsv.gz')
        self.assertTrue(os.path.isfile("out/test_nodes.tsv.gz"))
        self.assertTrue(len(node_metadata) > 0)
        os.remove("out/test_nodes.tsv.gz")

    def test_write_edges_pr(self):
        normal_dict = {}
        midpoint = int(len(self.curies) / 2)
        for curie in self.curies[:midpoint]:
            num_part = curie.split(':')[-1]
            normal_dict[curie] = {
                "id": {"label": f"label:{num_part}"},
                "type": [f"biolink:{num_part}"]
            }
        edge_metadata = targeted.write_edges(self.session, normal_dict, "out/test_edges.tsv.gz")
        self.assertTrue(os.path.isfile("out/test_edges.tsv.gz"))
        self.assertTrue(len(edge_metadata) > 0)
        os.remove("out/test_edges.tsv.gz")

    def test_write_edges_uniprot(self):
        normal_dict = {}
        midpoint = int(len(self.curies) / 2)
        for curie in self.curies[:midpoint]:
            num_part = curie.split(':')[-1]
            normal_dict[curie] = {
                "id": {"label": f"label:{num_part}"},
                "type": [f"biolink:{num_part}"]
            }
        edge_metadata = targeted.write_edges(self.session, normal_dict, "out/test_edges.tsv.gz", use_uniprot=True)
        self.assertTrue(os.path.isfile("out/test_edges.tsv.gz"))
        self.assertTrue(len(edge_metadata) > 0)
        os.remove("out/test_edges.tsv.gz")

    def test_create_kge_tarball(self):
        node_metadata = {}
        edge_metadata = {}
        targeted.create_kge_tarball('tmp', node_metadata, edge_metadata)
        self.assertTrue(os.path.isfile('targeted_assertions.tar.gz'))
        self.assertTrue(os.path.isfile('tmp/content_metadata.json'))
        self.assertTrue(os.path.isfile('tmp/nodes.tsv'))
        self.assertTrue(os.path.isfile('tmp/edges.tsv'))
        os.remove('tmp/content_metadata.json')
        os.remove('tmp/nodes.tsv')
        os.remove('tmp/edges.tsv')

    #endregion

#region Helper Methods

    def populate_db(self):
        (self.assertions, self.curies) = self.generate_assertion_records("data/curies.txt")
        self.entities = self.generate_entity_records("data/words.txt")
        self.evidence_list = self.generate_evidence_records(self.assertions, self.entities, "data/sentences.txt")
        self.evidence_scores = self.generate_evidence_score_records(self.evidence_list, ["biolink:entity_negatively_regulates_entity", "biolink:entity_positively_regulates_entity", "false"])
        self.session.add_all(self.assertions)
        self.session.add_all(self.entities)
        self.session.add_all(self.evidence_list)
        self.session.add_all(self.evidence_scores)
        self.session.commit()

    def generate_assertion_records(self, curie_filename: str, record_count: int=10) -> list[models.Assertion]:
        with open(curie_filename, 'r') as curie_file:
            curies = curie_file.read().splitlines()
        record_list = []
        curie_list = []
        id_hash = hashlib.sha1()
        for i in range(0, record_count - 1):
            subject_curie, object_curie = random.sample(curies, 2)
            id_hash.update((subject_curie + object_curie).encode('utf-8'))
            record_list.append(models.Assertion(id_hash.hexdigest()[:27], subject_curie, object_curie, 'biolink:ChemicalToGeneAssociation'))
            curie_list.append(subject_curie)
            curie_list.append(object_curie)
        # adding one PR curie with no UniProt equivalent, for test coverage
        subject_curie = 'CHEBI:24433'
        object_curie = 'PR:000000016'
        id_hash.update((subject_curie + object_curie).encode('utf-8'))
        record_list.append(models.Assertion(id_hash.hexdigest()[:27], subject_curie, object_curie,'biolink:ChemicalToGeneAssociation'))
        curie_list.append(subject_curie)
        curie_list.append(object_curie)
        curie_list = list(set(curie_list))
        return (record_list, curie_list)

    def generate_entity_records(self, word_filename: str, record_count: int=100) -> list[models.Entity]:
        record_list = []
        with open(word_filename, 'r') as word_file:
            words = word_file.read().splitlines()
        id_hash = hashlib.sha1()
        for i in range(0, record_count):
            word = random.sample(words, 1)[0]
            span = f"{random.randint(0,5)}|{len(word)}"
            id_hash.update((word + span).encode('utf-8'))
            record_list.append(models.Entity(id_hash.hexdigest()[:65], span, word))
        return record_list

    def generate_evidence_records(self, assertion_list: list[models.Assertion], entity_list: list[models.Entity], sentence_filename: str) -> list[models.Evidence]:
        record_list = []
        with open(sentence_filename, 'r') as sentence_file:
            sentences = sentence_file.read().splitlines()
        zones = ['abstract', 'document', 'sentence', 'title']
        id_hash = hashlib.sha1()
        for assertion in assertion_list:
            sentence = random.sample(sentences, 1)[0]
            subject_id, object_id = random.sample([entity.entity_id for entity in entity_list], 2)
            pmid = random.randint(10000000, 99999999)
            zone = random.sample(zones, 1)[0]
            year = random.randint(1975, 2022)
            id_hash.update((sentence + f"PMID:{pmid}").encode('utf-8'))
            record_list.append(models.Evidence(id_hash.hexdigest()[:65], assertion.assertion_id, f"PMID:{pmid}",
                                               sentence, subject_id, object_id, zone, "article", year))
        return record_list

    def generate_evidence_score_records(self, evidence_list: list[models.Evidence], predicate_list: list[str]) -> list[models.EvidenceScore]:
        record_list = []
        for evidence in evidence_list:
            for predicate in predicate_list:
                record_list.append(models.EvidenceScore(evidence.evidence_id, predicate, random.random()))
        return record_list

#endregion

    def tearDown(self):
        pass
