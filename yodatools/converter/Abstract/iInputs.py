from odm2api.ODMconnection import dbconnection
from odm2api.models import Base, setSchema


class iInputs(object):

    DB_VERSION = 2.0

    def __init__(self, **kwargs):

        if 'conn' in kwargs:
            self.conn = kwargs.pop('conn')

        else:
            self.conn = ':memory:'

        self.__init_session(self.conn)

    def parse(self, file_path):
        raise NotImplementedError()

    def verify(self):
        raise NotImplementedError()

    def sendODM2Session(self):
        # TODO: Remove method from this class and any other class that is implementing it
        raise NotImplementedError()

    def __init_session(self, conn):
        self._session_factory = dbconnection.createConnectionFromString(conn, self.DB_VERSION)
        self._session = self._session_factory.getSession()
        self._engine = self._session_factory.engine
        setSchema(self._engine)
        Base.metadata.create_all(self._engine)

