import unittest
from sqlalchemy import create_engine, engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base

Model = declarative_base(name='Model')


class ModelTestCase(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        self.session = Session(engine)
        Model.metadata.create_all(self.engine)
        # self.panel = Panel(1, 'ion torrent', 'start')
        # self.session.add(self.panel)
        # self.session.commit()

    def tearDown(self):
        Model.metadata.drop_all(self.engine)
