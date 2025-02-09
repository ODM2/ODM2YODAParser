import os
import openpyxl

from yodatools.converter.Abstract import iInputs
import time
from yodatools.excelparser.excelSpecimen import ExcelSpecimen
from yodatools.excelparser.excelTimeseries import ExcelTimeseries


class ExcelInput(iInputs):
    def __init__(self, **kwargs):
        super(ExcelInput, self).__init__(**kwargs)

        self.output_file = kwargs.get('output_file', 'export.csv')
        self.gauge = kwargs.get('gauge', None)


    @property
    def session(self):
        return self._session

    def parse(self, file_path):
        """
        If any of the methods return early, then check that they have the table ranges
        The table range should exist in the tables from get_table_name_range()
        :param file_path:
        :return:
        """
        self.file_path = file_path
        if not self.verify(self.file_path):
            print "Something is wrong with the file but what?"
            return False

        type_ = self.get_type(self.file_path)

        start = time.time()

        if type_ == "TimeSeries":
            # raise Exception("TimeSeries Parsing is not currently supported")
            et = ExcelTimeseries(self.file_path, self._session_factory, conn=self.conn)
            et.parse()
        else:
            es = ExcelSpecimen(self.file_path, self._session_factory)
            es.parse()


        # self._session.commit()

        end = time.time()
        print(end - start)

        return True

    def get_type(self, file_path):

        self.workbook = openpyxl.load_workbook(file_path, read_only=True)
        self.name_ranges = self.workbook.get_named_ranges()
        self.sheets = self.workbook.get_sheet_names()

        named_range = "TemplateProfile"
        range = self.workbook.get_named_range(named_range)
        sheet_name = "Instructions"
        sheet= self.workbook.get_sheet_by_name(sheet_name)
        cell = sheet[range.attr_text.split('!')[1].replace('$', '')]
        return cell.value


    def verify(self, file_path=None):

        if file_path is not None:
            self.input_file = file_path

        if not os.path.isfile(self.input_file):
            print "File does not exist"
            return False

        return True

    def sendODM2Session(self):  # this should be renamed to get not send because it returns a value
        # TODO: Remove method
        return self.session
