import unittest
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Iterator
import services

class ServicesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None: # pragma: no cover
        if 'tests' not in os.getcwd():
            os.chdir(f'{os.getcwd()}/tests')

    def setUp(self) -> None:
        self.engine = create_engine('sqlite:///:memory:')
        session = sessionmaker()
        session.configure(bind=self.engine)
        self.session = session()
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
            "CHEBI:5292": {
                "id": {
                    "identifier": "CHEBI:24433",
                    "label": "Geldanamycin"
                },
                "type": [
                    "biolink:SmallMolecule"
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
        result = services.update_node_metadata(node, initial, "infores:text-mining-provider-targeted")
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
        result = services.update_node_metadata(node, initial, "infores:text-mining-provider-targeted")
        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_update_edge_metadata_new_triple(self):
        expected = {
            "biolink:Protein|biolink:entity_negatively_regulates_entity|biolink:ChemicalEntity": {
                "subject": "biolink:ChemicalEntity",
                "predicate": "biolink:entity_negatively_regulates_entity",
                "object": "biolink:Protein",
                "relations": ["biolink:entity_negatively_regulates_entity"],
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
        result = services.update_edge_metadata(edge, initial, self.normalized_nodes, "infores:text-mining-provider-targeted")

        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_update_edge_metadata_new_relation(self):
        initial = {
            "biolink:Protein|biolink:entity_negatively_regulates_entity|biolink:ChemicalEntity": {
                "subject": "biolink:ChemicalEntity",
                "predicate": "biolink:entity_negatively_regulates_entity",
                "object": "biolink:Protein",
                "relations": ["biolink:entity_negatively_regulates_entity"],
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
                "relations": ["biolink:entity_negatively_regulates_entity"],
                "count": 2,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 2
                    }
                }
            }
        }
        edge = ["PR:000000015", "biolink:entity_negatively_regulates_entity", "CHEBI:24433", 'fake_id', "biolink:TestAssociation"]
        result = services.update_edge_metadata(edge, initial, self.normalized_nodes, "infores:text-mining-provider-targeted")

        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_update_edge_metadata_existing(self):
        initial = {
            "biolink:Protein|biolink:entity_negatively_regulates_entity|biolink:ChemicalEntity": {
                "subject": "biolink:ChemicalEntity",
                "predicate": "biolink:entity_negatively_regulates_entity",
                "object": "biolink:Protein",
                "relations": ["biolink:entity_negatively_regulates_entity"],
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
                "relations": ["biolink:entity_negatively_regulates_entity"],
                "count": 2,
                "count_by_source": {
                    "original_knowledge_source": {
                        "infores:text-mining-provider-targeted": 2
                    }
                }
            }
        }
        edge = ["PR:000000015", "biolink:entity_negatively_regulates_entity", "CHEBI:24433", 'fake_id', "biolink:ChemicalToGeneAssociation"]
        result = services.update_edge_metadata(edge, initial, self.normalized_nodes, "infores:text-mining-provider-targeted")
        self.assertEqual(result, expected)
        self.assertEqual(result, initial)

    def test_get_category_default_category(self):
        self.assertEqual(services.get_category("CHEBI:24444", self.normalized_nodes), "biolink:NamedThing")

    def test_get_category_default_category_drugbank(self):
        self.assertEqual(services.get_category("DRUGBANK:24444", self.normalized_nodes), "biolink:SmallMolecule")

    def test_get_category_found(self):
        self.assertEqual(services.get_category("CHEBI:24433", self.normalized_nodes), "biolink:ChemicalEntity")

    def test_is_normal_true(self):
        self.assertTrue(services.is_normal("PR:000000015", self.normalized_nodes))

    def test_is_normal_false(self):
        self.assertFalse(services.is_normal("PR:000000016", self.normalized_nodes))

    def test_get_kgx_nodes_type_check(self):
        curies = ['CHEBI:5292', 'PR:000000015', 'UniProtKB:P19883']
        results = services.get_kgx_nodes(curies, self.normalized_nodes)
        self.assertIsInstance(results, Iterator)
        for item in results:
            self.assertIsInstance(item, list)
            self.assertEqual(len(item), 3)

    # def test_get_kgx_nodes_default_category(self):
    #     result_iterator = services.get_kgx_nodes(['CHEBI:24444'], self.normalized_nodes)
    #     result = next(result_iterator)
    #     self.assertEqual(result, ['CHEBI:24444', 'test_chebi', 'biolink:NamedThing'])

    def test_get_kgx_nodes_default_category_drugbank(self):
        result_iterator = services.get_kgx_nodes(['DRUGBANK:24444'], self.normalized_nodes)
        result = next(result_iterator)
        self.assertEqual(result, ['DRUGBANK:24444', 'test_drugbank', 'biolink:SmallMolecule'])

    def test_get_kgx_nodes_not_normal(self):
        result_iterator = services.get_kgx_nodes(['PR:000000016'], self.normalized_nodes)
        result = next(result_iterator)
        self.assertEqual(result, [])

    def test_get_kgx_nodes_normal(self):
        result_iterator = services.get_kgx_nodes(['CHEBI:5292'], self.normalized_nodes)
        result = next(result_iterator)
        self.assertEqual(result, ['CHEBI:5292', 'Geldanamycin', 'biolink:SmallMolecule'])
