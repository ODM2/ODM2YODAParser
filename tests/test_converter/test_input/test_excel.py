import os
import unittest

import openpyxl

from yodatools.converter.Inputs.excelInput import ExcelInput
from odm2api.models import People, SamplingFeatures

class ExcelTest(unittest.TestCase):

    def setUp(self):
        self.before_each_do()

    def before_each_do(self):
        self.curr_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

        self.excel = ExcelInput()


    def test_parse_specimen(self):
        # D:\DEV\YODA - Tools\tests\test_files\test_ts_specimen_output.yaml
        file_path = os.path.join(self.curr_folder, 'test_files', "YODA_SpecimenTimeSeries_Template_0.3.1-alpha_LR_example.xlsx")
        # file_path = os.path.join(curr_folder, 'test_files', 'test_ts_specimen_output.yaml')

        self.excel.parse(file_path)
        session = self.excel.sendODM2Session()

        assert session != None
        assert len(session.query(People).all()) > 0
        assert len(session.query(SamplingFeatures).all()) > 0
        session.close()


    def test_parse_ts(self):
        file_path = os.path.join(self.curr_folder, 'test_files', 'YODA_v0.3.3_TS_climate(wHeaders).xlsm')
        # file_path = os.path.join(curr_folder, 'test_files', 'test_ts_output.yaml')
        self.excel.parse(file_path)

        session = self.excel.sendODM2Session()

        assert session != None

        assert len(session.query(People).all()) > 0
        assert len(session.query(SamplingFeatures).all()) > 0
        session.close()

