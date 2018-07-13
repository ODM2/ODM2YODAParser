from odm2api.ODMconnection import dbconnection
from odm2api.models import Base, setSchema


class iInputs(object):

    DB_VERSION = 2.0

    def __init__(self, **kwargs):

        if 'conn' in kwargs:
            conn = kwargs.pop('conn')

        else:
            conn = ':memory:'

        # self.create_memory_db()
        self.__init_session__(conn)

    def parse(self, file_path):
        raise NotImplementedError()

    def verify(self):
        raise NotImplementedError()

    def sendODM2Session(self):
        # TODO: Remove method from this class and any other class that is implementing it
        raise NotImplementedError()

    # def create_memory_db(self):
    #     # create connection to temp sqlite db
    #     # self._session_factory = dbconnection.createConnection('sqlite', r'D:\DEV\YODA-Tools\tests\test_files\ODM2_ts.sqlite', 2.0)
    #     self._session_factory = dbconnection.createConnection('sqlite', ':memory:', 2.0)
    #     self._session = self._session_factory.getSession()
    #     self._engine = self._session_factory.engine
    #     setSchema(self._engine)
    #     Base.metadata.create_all(self._engine)

    def __init_session__(self, conn):

        # if engine == 'sqlite':
        # conn = conn.replace('{engine}:///'.format(engine=engine), '')

        # self._session_factory = dbconnection.createConnection(engine, conn, self.DB_VERSION)
        self._session_factory = dbconnection.createConnectionFromString(conn, self.DB_VERSION)
        self._session = self._session_factory.getSession()
        self._engine = self._session_factory.engine
        setSchema(self._engine)
        Base.metadata.create_all(self._engine)

