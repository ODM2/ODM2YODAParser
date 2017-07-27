# import os
#
# from yodatools.timeseries import convertTimeSeries
#
# from yodatools.converter.Inputs.yamlInput import yamlInput
# from yodatools.converter.Inputs.excelInput import ExcelInput
# from yodatools.converter.Outputs.dbOutput import dbOutput
# from yodatools.converter.Outputs.yamlOutput import yamlOutput
#
#
# from tests.test_util import build_ts_session
#
# import openpyxl
#
#
#
# class TestTimeseries:
#
#
#
#     curr_folder = os.path.abspath(os.path.dirname(__file__))
#
#     # def setup(self):
#     #     print self.curr_folder
#     #     self.ct = convertTimeSeries()
#
#
#     def test_create_object_yaml(self):
#         file_path = os.path.join(self.curr_folder, 'test_files', 'test_ts.yaml')
#         convertTimeSeries.serial_to_object()
#
#
#     def test_create_object_excel(self):
#         file_path = os.path.join(self.curr_folder, 'test_files', 'YODA_v0.3.3_TS_climate(wHeaders).xlsm')
#         self.workbook = openpyxl.load_workbook(file_path, read_only=True)
#         self.name_ranges = self.workbook.get_named_ranges()
#         self.sheets = self.workbook.get_sheet_names()
#         convertTimeSeries.serial_to_object()
#
#
#     def test_create_serial_ODM2(self):
#
#         session= build_ts_session()
#         do = dbOutput()
#         objs=do.parseObjects(session)
#         print objs.keys()
#         # convertTimeSeries.object_to_series(objs["timeseriesresultvalues"])
#
#
#     def test_create_serial_yaml(self):
#
#         session = build_ts_session()
#         yo = yamlOutput()
#         objs= yo.parseObjects(session)
#         print objs.keys()
#         # convertTimeSeries.object_to_series(objs["timeseriesresultvalues"])
#
#
#
#
