import os

from tests.test_util import build_ts_session, build_ts_specimen_session
from yodatools.converter.Outputs.dbOutput import dbOutput
from odm2api.ODMconnection import dbconnection
from odm2api.ODM2.models import People, SamplingFeatures, MeasurementResultValues, TimeSeriesResultValues


curr_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# curr_folder = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
# file_path = os.path.join(curr_folder, 'test_files', 'YODA_TimeSeriesSpecimen_RB_2014-15_pub.xlsx')
class TestDb:
    def setup(self):

        self.connection_string = 'mysql+pymysql://ODM:odm@localhost/odm2'
        # self.connection_string = 'sqlite://'
        self.do = dbOutput()
        session_factory = dbconnection.createConnectionFromString(self.connection_string)
        self.session_out = session_factory.getSession()



    # def test_create_specimen(self):
    #     session = build_ts_specimen_session()
    #     self.do.save(session, self.connection_string)
    #
    #     assert len(self.session_out.query(People).all()) > 0
    #     assert len(self.session_out.query(SamplingFeatures).all()) > 0
    #     assert self.session_out.query(MeasurementResultValues).first() is not None
    #     self.session_out.close()


    def test_create_ts(self):
        session = build_ts_session()
        self.do.save(session, self.connection_string)

        assert len(self.session_out.query(People).all()) > 0
        assert len(self.session_out.query(SamplingFeatures).all()) > 0
        assert self.session_out.query(TimeSeriesResultValues).first() is not None
        self.session_out.close()
