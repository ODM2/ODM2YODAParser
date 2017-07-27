import os

from yodatools.converter.Inputs.yamlInput import yamlInput

curr_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

class TestYaml:
    def setup(self):
        self.yi = yamlInput()

    def test_parse_ts(self):
        file_path = os.path.join(curr_folder, 'test_files', 'test_ts.yaml')
        # file_path = os.path.join(curr_folder, 'test_files', 'test_ts_output.yaml')
        self.yi.parse(file_path)

        session = self.yi.sendODM2Session()

        assert session is not None
        from odm2api.ODM2.models import People, SamplingFeatures, TimeSeriesResultValues
        assert len(session.query(People).all()) > 0
        assert len(session.query(SamplingFeatures).all()) > 0
        assert session.query(TimeSeriesResultValues).first() is not None
        session.close()

    # def test_parse_specimen(self):
    #
    #     file_path = os.path.join(curr_folder, 'test_files', 'test_specimen_ts.yaml')
    #
    #     self.yi.parse(file_path)
    #     session = self.yi.sendODM2Session()
    #
    #     assert session is not None
    #     from odm2api.ODM2.models import People, SamplingFeatures, MeasurementResultValues
    #     assert len(session.query(People).all()) > 0
    #     assert len(session.query(SamplingFeatures).all()) > 0
    #     assert session.query(MeasurementResultValues).first() is not None
    #     session.close()



