import os
import unittest

import openpyxl

from yodatools.converter.Inputs.excelInput import ExcelInput
from odm2api.ODM2.models import People, SamplingFeatures, MeasurementResultValues, TimeSeriesResultValues

class TestExcel:

    def setup(self):
        self.curr_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        self.excel = ExcelInput()

    def test_parse_ts(self):
        file_path = os.path.join(self.curr_folder, 'test_files', 'YODA_v0.3.3_TS_climate(wHeaders).xlsm')
        self.excel.parse(file_path)

        session = self.excel.sendODM2Session()

        assert session is not None

        assert len(session.query(People).all()) > 0
        assert len(session.query(SamplingFeatures).all()) > 0
        assert session.query(TimeSeriesResultValues).first() is not None
        session.close()


    def test_parse_specimen(self):
        # D:\DEV\YODA - Tools\tests\test_files\test_ts_specimen_output.yaml
        # file_path = os.path.join(self.curr_folder, 'test_files', 'YODA_TimeSeriesSpecimen_RB_2014-15_pub.xlsx')
        file_path = os.path.join(self.curr_folder, 'test_files',
                                 'YODA_SpecimenTimeSeries_Template_0.3.1-alpha_LR_example.xlsx')
                                 # 'YODA_TimeSeriesSpecimen_RB_2014-15_pub.xlsx')

        self.excel.parse(file_path)
        session = self.excel.sendODM2Session()

        assert session is not None
        people = session.query(People).all()
        numpeople = len(people)
        # print numpeople
        # print people
        assert numpeople > 0
        assert len(session.query(SamplingFeatures).all()) > 0
        assert session.query(MeasurementResultValues).first() is not None
        session.close()
