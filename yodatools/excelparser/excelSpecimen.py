import os
import openpyxl
from openpyxl.worksheet.table import Table
from openpyxl.cell.cell import Cell
from openpyxl.workbook.workbook import Workbook
from uuid import uuid4

from odm2api.models import *
from yodatools.converter.Abstract import iInputs
from pandas import DataFrame
import pandas as pd
import numpy as np
import time
import string
import re
from sqlalchemy.exc import IntegrityError

from .ExcelParser import ExcelParser


class ExcelSpecimen(ExcelParser):
    def __init__(self, input_file, **kwargs):

        super(ExcelSpecimen, self).__init__()

        self.input_file = input_file

        self.gauge = None
        self.total_rows_to_read = 0
        self.rows_read = 0

        self.gauge = kwargs.get('gauge', None)

        self.workbook = None
        self.sheets = []
        self.name_ranges = {}
        self.tables = {}
        self._init_data(input_file)

        self._orgs = {}

    @property
    def session(self):
        if hasattr(self, '_session'):
            return self._session
        return None

    def get_table_name_ranges(self):
        """
        Returns a list of the name range that have a table.
        The name range should contain the cells locations of the data.
        :rtype: list
        """

        table_name_range = {}
        for name_range in self.name_ranges:
            name = name_range.name  # type: str

            if '_table' in name.lower():
                name = re.sub(r'_[tT]able', '', name)

            if any(name.lower() in table_name.lower() for table_name in self.TABLE_NAMES):
                sheet, dimensions = name_range.attr_text.split('!')
                sheet = sheet.replace('\'', '')

                if sheet in table_name_range:
                    table_name_range[sheet].append(name_range)
                else:
                    table_name_range[sheet] = [name_range]

                self.count_number_of_rows_to_parse(dimensions=dimensions)

        return table_name_range

    def _init_data(self, file_path):
        self.workbook = openpyxl.load_workbook(file_path, data_only=True)  # type: Workbook

        # Loop through worksheets to grab table data
        for ws in self.workbook.worksheets:

            try:
                tables = getattr(ws, '_tables', [])
            except IndexError:
                continue

            for table in tables:  # type: Table

                rows = ws[table.ref]

                if table.name == 'DatasetInformation':
                    # The DatasetInformation table does not have
                    # headers so it must be handled differently
                    df = DataFrame([[cell.value for cell in row] for row in rows])

                    headers = df[0].tolist()  # Headers are in the first column
                    data = [df[1].tolist(), ]  # data values are in the second column

                else:

                    # check if table_rows length is less than 2, since the first row is just the table headers
                    if len(rows) < 2:
                        continue

                    headers = [cell.value for cell in rows[0]]  # get headers from the first row
                    data = [[cell.value for cell in row] for row in rows[1:]]  # get values from 2...n rows

                headers = map(lambda x: x.strip(), headers)  # remove leading/trailing whitespaces from headers...

                self.tables[table.name.strip()] = DataFrame(data, columns=headers).dropna(how='all')

        self.workbook.close()

        # TODO: If this whole table nonsense works out... don't think we'll need this stuff.
        self.workbook = openpyxl.load_workbook(file_path, read_only=True)

        self.name_ranges = self.workbook.get_named_ranges()
        self.sheets = self.workbook.get_sheet_names()

    def count_number_of_rows_to_parse(self, dimensions):
        # http://stackoverflow.com/questions/1450897/python-removing-characters-except-digits-from-string
        top, bottom = dimensions.replace('$', '').split(':')
        all = string.maketrans('', '')
        nodigs = all.translate(all, string.digits)
        top = int(top.translate(all, nodigs))
        bottom = int(bottom.translate(all, nodigs))
        self.total_rows_to_read += (bottom - top)


    def get_range_address(self, named_range):
        if named_range is not None:
            return named_range.attr_text.split('!')[1].replace('$', '')
        return None

    def get_range_value(self, range_name, sheet):
        value = None
        named_range = self.workbook.get_named_range(range_name)
        range_ = self.get_range_address(named_range)
        if range_:
            value = sheet[range_].value
        return value


    def parse(self, session_factory):
        """
        If any of the methods return early, then check that they have the table ranges
        The table range should exist in the tables from get_table_name_range()
        :param :
        :return:
        """

        self._session = session_factory.getSession()
        self._engine = session_factory.engine

        # self.tables = self.get_table_name_ranges()

        start = time.time()

        self.parse_people_and_orgs_sheet()
        self.parse_datasets()
        self.parse_methods()
        self.parse_variables()
        self.parse_units()
        self.parse_processing_level()
        self.parse_sampling_feature()
        self.parse_specimens()
        self.parse_analysis_results()

        self._session.commit()

        end = time.time()
        print(end - start)

        return True

    def parse_datasets(self):

        dataset_table = self.tables.get('DatasetInformation')

        for _, row in dataset_table.iterrows():

            params = {
                'DataSetUUID': row.get('Dataset UUID'),
                'DataSetTypeCV': row.get('Dataset Type [CV]'),
                'DataSetCode': row.get('Dataset Code'),
                'DataSetTitle': row.get('Dataset Title'),
                'DataSetAbstract': row.get('Dataset Abstract')
            }

            self.data_set = self.get_or_create(DataSets, params, filter_by='DataSetUUID')

        # dataset = DataSets()
        # dataset.DataSetUUID = self.get_range_value("DatasetUUID", sheet)
        # dataset.DataSetTypeCV = self.get_range_value("DatasetType", sheet)
        # dataset.DataSetCode = self.get_range_value("DatasetCode", sheet)
        # dataset.DataSetTitle = self.get_range_value("DatasetTitle", sheet)
        # dataset.DataSetAbstract = self.get_range_value("DatasetType", sheet)
        # self._session.add(dataset)
        #
        # self._flush()

    def parse_analysis_results(self):
        SHEET_NAME = "Analysis_Results"
        sheet, tables = self.get_sheet_and_table(SHEET_NAME)

        if not len(tables):
            print "No analysis result found"
            return

        for table in tables:
            rows = sheet[self.get_range_address(table)]
            for row in rows:

                action = Actions()
                feat_act = FeatureActions()
                act_by = ActionBy()
                measure_result = MeasurementResults()
                measure_result_value = MeasurementResultValues()
                related_action = RelatedActions()
                dataset_result = DataSetsResults()

                # Action
                method = self._session.query(Methods).filter_by(MethodCode=row[7].value).first()
                action.MethodObj = method
                action.ActionTypeCV = "Specimen analysis"
                action.BeginDateTime = row[5].value
                action.BeginDateTimeUTCOffset = row[6].value

                # Feature Actions
                # TODO: row[0] appears to be a ResultUUID, not a SamplingFeatureUUID... fix it...?
                sampling_feature = self._session.query(SamplingFeatures)\
                    .filter_by(SamplingFeatureCode=str(row[1].value))\
                    .first()
                    # .filter(SamplingFeatureCode=row[1].value)\

                feat_act.SamplingFeatureObj = sampling_feature
                feat_act.ActionObj = action

                # Action By
                try:
                    last_name = row[8].value.split(' ')[-1]
                except IndexError as e:
                    print(e)
                    last_name = row[8].value

                person = self._session.query(People).filter_by(PersonLastName=last_name).first()
                affiliations = self._session.query(Affiliations).filter_by(PersonID=person.PersonID).first()
                act_by.AffiliationObj = affiliations
                act_by.ActionObj = action
                act_by.IsActionLead = True

                related_action.ActionObj = action
                related_action.RelationshipTypeCV = "Is child of"
                collectionAction = self._session.query(FeatureActions)\
                    .filter(FeatureActions.FeatureActionID == SamplingFeatures.SamplingFeatureID)\
                    .filter(SamplingFeatures.SamplingFeatureCode == str(row[1].value))\
                    .first()

                related_action.RelatedActionObj = collectionAction.ActionObj

                self._session.add(action)
                self._session.add(feat_act)
                self._session.add(act_by)
                self._session.add(related_action)

                # Measurement Result (Different from Measurement Result Value) also creates a Result
                variable = self._session.query(Variables).filter_by(VariableCode=row[2].value).first()
                units_for_result = self._session.query(Units).filter_by(UnitsName=row[4].value).first()
                proc_level = self._session.query(ProcessingLevels).filter_by(ProcessingLevelCode=row[11].value).first()

                units_for_agg = self._session.query(Units).filter_by(UnitsName=row[14].value).first()
                measure_result.CensorCodeCV = row[9].value
                measure_result.QualityCodeCV = row[10].value
                measure_result.TimeAggregationInterval = row[13].value
                measure_result.TimeAggregationIntervalUnitsObj = units_for_agg
                measure_result.AggregationStatisticCV = row[15].value
                measure_result.ResultUUID = row[0].value
                measure_result.FeatureActionObj = feat_act
                measure_result.ResultTypeCV = "Measurement"
                measure_result.VariableObj = variable
                measure_result.UnitsObj = units_for_result
                measure_result.ProcessingLevelObj = proc_level
                measure_result.StatusCV = "Complete"
                measure_result.SampledMediumCV = row[12].value
                measure_result.ValueCount = 1
                measure_result.ResultDateTime = collectionAction.ActionObj.BeginDateTime
                self._session.add(measure_result)
                self._session.flush()


                #DataSet Results
                if self.data_set is not None:
                    dataset_result.DataSetObj = self.data_set
                    dataset_result.ResultObj = measure_result
                    self._session.add(dataset_result)

                # Measurements Result Value
                measure_result_value.DataValue = row[3].value
                measure_result_value.ValueDateTime = collectionAction.ActionObj.BeginDateTime
                measure_result_value.ValueDateTimeUTCOffset = collectionAction.ActionObj.BeginDateTimeUTCOffset
                measure_result_value.ResultObj = measure_result





                self._session.add(measure_result_value)

                self._flush()

                self._updateGauge()

    # def parse_sites(self):
    #     return self.parse_sampling_feature()

    def parse_units(self):
        table = self.tables.get('Units', DataFrame())
        for _, row in table.iterrows():

            params = {
                'UnitsTypeCV': row.get('Units Type [CV]'),
                'UnitsAbbreviation': row.get('Units Abbreviation'),
                'UnitsName': row.get('Units Name'),
                'UnitsLink': row.get('Units Link')
            }

            _ = self.get_or_create(Units, params, filter_by=['UnitsName', 'UnitsAbbreviation', 'UnitsTypeCV'],
                                   check_fields=['UnitsTypeCV'])

            self._updateGauge()

    def parse_people_and_orgs_sheet(self):

        # Create Organization objects
        organization_table = self.tables.get('Organizations', DataFrame())
        for _, row in organization_table.iterrows():
            params = {
                'OrganizationTypeCV': row.get('Organization Type [CV]'),
                'OrganizationCode': row.get('Organization Code'),
                'OrganizationName': row.get('Organization Name'),
                'OrganizationDescription': row.get('Organization Description'),
                'OrganizationLink': row.get('Organization Link'),
            }

            org = self.get_or_create(Organizations, params, filter_by='OrganizationName')
            self._orgs[row.get('Organization Name')] = org  # save this for later when we create Affiliations


        # Create Person and Affiliation objects
        people_table = self.tables.get('People', DataFrame())
        for _, row in people_table.iterrows():  # type: (any, DataFrame)

            row.fillna(value='', inplace=True)  # replace NaN values with empty string

            person_params = {
                'PersonFirstName': row.get('First Name'),
                'PersonLastName': row.get('Last Name'),
                'PersonMiddleName': row.get('Middle Name')
            }

            person = self.get_or_create(People, person_params)

            aff_params = {
                'AffiliationStartDate': row.get('Affiliation Start Date'),
                'AffiliationEndDate': row.get('Affiliation End Date'),
                'PrimaryPhone': row.get('Primary Phone'),
                'PrimaryEmail': row.get('Primary Email'),
                'PrimaryAddress': row.get('Primary Address'),
                'PersonLink': row.get('Person Link'),
                'OrganizationObj': self._orgs.get(row.get('Organization Name')),
                'PersonObj': person
            }

            _ = self.get_or_create(Affiliations, aff_params, filter_by='PersonID')

    def get_sheet_and_table(self, sheet_name):
        sheet = self.workbook.get_sheet_by_name(sheet_name)
        return sheet, self.tables.get(sheet_name, [])

    def parse_processing_level(self):

        table = self.tables.get('ProcessingLevels', DataFrame())
        for _, row in table.iterrows():

            params = {
                'ProcessingLevelCode': row.get('Processing Level Code'),
                'Definition': row.get('Definition'),
                'Explanation': row.get('Explanation')
            }

            _ = self.get_or_create(ProcessingLevels, params, filter_by=['ProcessingLevelCode'])

            self._updateGauge()

    def parse_sampling_feature(self):

        elevation_datum_range = self.workbook.defined_names['ElevationDatum'].destinations
        elevation_datum = self.get_named_range_value(*next(elevation_datum_range))

        latlon_datum_range = self.workbook.defined_names['LatLonDatum'].destinations
        latlon_datum = self.get_named_range_value(*next(latlon_datum_range))

        # TODO: The SpatialReferences table does not exist in current excel templates... seek guidance young one.
        # Currently the fix is to get/create a new record using latlon_datum as the SRS code and name...
        spatial_ref = self.get_or_create(SpatialReferences, {'SRSCode': latlon_datum, 'SRSName': latlon_datum})

        table = self.tables.get('Sites', DataFrame())
        for _, row in table.iterrows():

            params = {
                'SamplingFeatureUUID': str(uuid4()),  # Adding UUID in excel templates is redundant
                'SamplingFeatureCode': row.get('Sampling Feature Code'),
                'SamplingFeatureName': row.get('Sampling Feature Name'),
                'SamplingFeatureDescription': row.get('Sampling Feature Description'),
                'FeatureGeometryWKT': row.get('Feature Geometry WKT'),
                'Elevation_m': row.get('Elevation_m'),
                'SamplingFeatureTypeCV': 'Site',
                'SiteTypeCV': row.get('Site Type [CV]'),
                'Latitude': row.get('Latitude'),
                'Longitude': row.get('Longitude'),
                'ElevationDatumCV': elevation_datum,
                'SpatialReferenceObj': spatial_ref
            }

            _ = self.get_or_create(Sites, params, filter_by=['SamplingFeatureCode'])

        self._updateGauge()

    def parse_spatial_reference(self):
        SHEET_NAME = "SpatialReferences"
        sheet, tables = self.get_sheet_and_table(SHEET_NAME)

        if not len(tables):
            return []

        spatial_references = {}
        for table in tables:
            cells = sheet[self.get_range_address(table)]
            for row in cells:
                sr = SpatialReferences()
                sr.SRSCode = row[0].value
                sr.SRSName = row[1].value
                sr.SRSDescription = row[2].value
                sr.SRSLink = row[3].value

                spatial_references[sr.SRSName] = sr

        return spatial_references

    def parse_specimens(self):
        SPECIMENS = 'Specimens'
        sheet, tables = self.get_sheet_and_table(SPECIMENS)

        if not len(tables):
            print "No specimens found"
            return []

        with self.session.no_autoflush:
            for table in tables:
                rows = sheet[self.get_range_address(table)]

                for row in rows:
                    specimen = Specimens()
                    action = Actions()
                    related_feature = RelatedFeatures()
                    feature_action = FeatureActions()

                    # First the Specimen/Sampling Feature
                    specimen.SamplingFeatureUUID = row[0].value
                    specimen.SamplingFeatureCode = row[1].value
                    specimen.SamplingFeatureName = row[2].value
                    specimen.SamplingFeatureDescription = row[3].value
                    specimen.SamplingFeatureTypeCV = "Specimen"
                    specimen.SpecimenMediumCV = row[5].value
                    specimen.IsFieldSpecimen = row[6].value
                    specimen.ElevationDatumCV = 'Unknown'
                    specimen.SpecimenTypeCV = row[4].value
                    specimen.SpecimenMediumCV = 'Liquid aqueous'

                    # Related Features
                    related_feature.RelationshipTypeCV = 'Was Collected at'

                    try:
                        sampling_feature = self._session.query(SamplingFeatures).filter_by(SamplingFeatureCode=row[7].value).first()
                    except IntegrityError as e:
                        print(e)
                        continue

                    related_feature.SamplingFeatureObj = specimen
                    related_feature.RelatedFeatureObj = sampling_feature

                    # Last is the Action/SampleCollectionAction
                    action.ActionTypeCV = 'Specimen collection'
                    action.BeginDateTime = row[8].value
                    action.BeginDateTimeUTCOffset = row[9].value
                    method = self._session.query(Methods).filter_by(MethodCode=row[10].value).first()
                    action.MethodObj = method

                    feature_action.ActionObj = action
                    feature_action.SamplingFeatureObj = specimen

                    self._session.add(specimen)
                    self._session.add(action)
                    self._session.add(related_feature)
                    self._session.add(feature_action)

                    try:
                        self.session.commit()
                    except IntegrityError as e:
                        print(e)
                        session.rollback()

                    self._updateGauge()

        # self._session.flush()  # Need to set the RelatedFeature.RelatedFeatureID before flush will work
        self._flush()

    def parse_methods(self):
        """
        Parse methods recorded in the excel template

        NOTE: When parsing SpecimenTimeSeries templates, there are two
        seperate tables - the SpecimenCollectionMethods table, and the
        SpecimenAnalysisMethods table. `parse_methods()` parses both
        of tables.
        :return:
        """

        def parse_method(row):
            params = {
                'MethodTypeCV': row.get('Method Type [CV]'),
                'MethodCode': row.get('Method Code'),
                'MethodName': row.get('Method Name'),
                'MethodDescription': row.get('Method Description'),
                'OrganizationObj': self._orgs.get(row.get('Organization Name'))
            }

            # check if params has all required fields, if not, then raise assertion error
            assert(all(params.values()))

            # After checking for required fields, add the non required field
            params.update(MethodLink=row.get('Method Link'))

            _ = self.get_or_create(Methods, params, filter_by='MethodCode')

        collections_method_table = self.tables.get('SpecimenCollectionMethods')
        analysis_methods_table = self.tables.get('SpecimenAnalysisMethods')

        methods_table = collections_method_table.append(analysis_methods_table)  # type: DataFrame

        # drop rows where *all* values in the row are NaN


        for _, row in methods_table.iterrows():
            try:
                parse_method(row)
            except AssertionError:
                continue
            self._updateGauge()


    def parse_variables(self):

        table = self.tables.get('Variables', DataFrame())
        for _, row in table.iterrows():

            params = {
                'VariableTypeCV': row.get('Variable Type [CV]'),
                'VariableCode': row.get('Variable Code'),
                'VariableNameCV': row.get('Variable Name [CV]'),
                'VariableDefinition': row.get('Variable Definition'),
                'SpeciationCV': row.get('Speciation [CV]'),
                'NoDataValue': row.get('No Data Value')
            }

            _ = self.get_or_create(Variables, params, filter_by=['VariableCode'], check_fields=['NoDataValue'])
