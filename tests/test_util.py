
from odm2api.models import Base
from odm2api.ODMconnection import dbconnection
import os
curr_folder = os.path.abspath(os.path.dirname(__file__))


def build_ts_db():
    # create connection to temp sqlite db
    db_path = os.path.join(curr_folder, 'test_files', 'ODM2_ts.sqlite')
    session_factory = dbconnection.createConnection('sqlite', db_path, 2.0)
    # _engine = session_factory.engine
    return session_factory


def build_ts_session():
    session_factory = build_ts_db()
    session = session_factory.getSession()
    return session


def build_ts_sepecimen_db():
    # create connection to temp sqlite db
    db_path = os.path.join(curr_folder, 'test_files', 'ODM2_ts_specimen.sqlite')
    session_factory = dbconnection.createConnection('sqlite', db_path, 2.0)
    # _engine = session_factory.engine
    return session_factory


def build_ts_specimen_session():
    session_factory = build_ts_sepecimen_db()
    session = session_factory.getSession()
    return session







