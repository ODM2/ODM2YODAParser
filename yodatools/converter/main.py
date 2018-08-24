# !/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Stephanie'


import time
import sys

from argparse import ArgumentParser

from yodatools.converter.Inputs.excelInput import ExcelInput
from yodatools.converter.Inputs.yamlInput import yamlInput
from yodatools.converter.Outputs.dbOutput import dbOutput
from yodatools.converter.Outputs.yamlOutput import yamlOutput


def run_converter(input_file, yoda_file=None, db_conn=None, excel_file=None):
    yoda_output_file_path = yoda_file
    db_output_connection = db_conn
    excel_output_file_path = excel_file

    # Check if it is a yaml, or excel file
    file_type = verify_file_type(input_file)

    if file_type == 'invalid':  # Accept only excel and yaml files
        print('File extension isvalid or no file')
        return
    try:
        if file_type == 'excel':
            excel = ExcelInput(input_file=input_file)
            excel.parse()
            session = excel.sendODM2Session()
        else:
            # Must be a yoda file
            yoda = yamlInput(input_file)
            yoda.parse(input_file)
            session = yoda.sendODM2Session()

        # Go through each type
        if yoda_output_file_path is not None:
            yaml = yamlOutput()
            yaml.save(session=session, file_path=yoda_output_file_path)

        if db_output_connection is not None:
            db = dbOutput()
            db.save(session=session, connection_string=db_output_connection)

        if excel_output_file_path is not None:
            print('excel output is not yet supported.')

        # if 'odm2' in selections:
        #     print('export to odm2')
        #     """
        #     create connection string
        #     call dboutput and do same as yoda export and send in
        #     connection string as filepath
        #     """

        session.close_all()
        print('Conversion of the datafile has completed')
        return True
    except Exception as e:
        print('error parsing values {}'.format(e.message))
        raise e


def verify_file_type(input_file):
    CONST_LEGAL_EXCEL_EXTENSIONS = ('xlsx', 'xlsm')

    if input_file.endswith(CONST_LEGAL_EXCEL_EXTENSIONS):
        file_type = 'excel'
    elif input_file.endswith('yml') or input_file.endswith('yaml') or input_file.endswith('yoda'):  # noqa
        file_type = 'yaml'
    else:
        file_type = 'invalid'

    return file_type


# command line stuff
# Application entry point.
if __name__ == '__main__':
    parser = ArgumentParser(description='YodaTools')

    parser.add_argument(
        '-i', '--input',
        dest='inputFile',
        help="""\
        Specify a file in of one of these formats:
        1. A single yodaparser (.yaml) file.
        2. An Excel YODA template.
        """,
        required=True,
        action='store'
    )
    # parser.add_argument(
    #    '-e', '--excel',
    #    dest='excelFile',
    #    help='Specify a single excel path to save the converted data too.',
    #    required=False,
    #    action='store'
    # )
    parser.add_argument(
        '-y', '--yoda',
        dest='yodaFile',
        help='Specify a single yoda file path to save the converted data too.',
        required=False,
        action='store'
    )
    parser.add_argument(
        '-d', '--database',
        dest='dbConn',
        help="""\
        Specify a Database connection.
        Format: {engine}+{driver}://{user}:{pass}@{address}/{db}
        mysql+pymysql://ODM:odm@localhost/odm2
        mssql+pyodbc://ODM:123@localhost/odm2
        postgresql+psycopg2://ODM:odm@test.uwrl.usu.edu/odm2
        sqlite+pysqlite:///path/to/file
        """,
        required=False,
        action='store'
    )
    args = parser.parse_args()

    start_time = time.time()
    run_converter(args.inputFile, args.yodaFile, args.dbConn)

    # logger = logging.getLogger('YODA_logger')
    # logger.info("Running time: %s seconds" % (time.time() - start_time))
