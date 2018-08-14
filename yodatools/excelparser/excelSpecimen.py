import os
from collections import defaultdict
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
from sqlalchemy.orm.exc import NoResultFound

from .ExcelParser import ExcelParser


class ExcelSpecimen(ExcelParser):
    def __init__(self, input_file, session_factory, **kwargs):

        super(ExcelSpecimen, self).__init__(input_file, session_factory, **kwargs)


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

                # check if table_rows length is less than 2, since the first row is just the table headers
                if len(rows) < 2:
                    continue

                # get headers from the first row and remove leading/trailing whitespaces
                headers = map(lambda x: x.strip(), [cell.value for cell in rows[0]])

                # get values from 2...n rows
                data = [[cell.value for cell in row] for row in rows[1:]]

                self.tables[table.name.strip()] = DataFrame(data, columns=headers).dropna(how='all')

        self.total_rows_to_read = self.calc_total_rows()

        self.workbook.close()

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


    def parse(self):
        """
        Parses the excel file read in self._init_data
        :param :
        :return:
        """

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

        end = time.time()
        print(end - start)

        return True

    def parse_datasets(self):

        dataset_uuid = self.get_named_range_cell_value('DatasetUUID')
        dataset_type = self.get_named_range_cell_value('DatasetType')
        dataset_code = self.get_named_range_cell_value('DatasetCode')
        dataset_title = self.get_named_range_cell_value('DatasetTitle')
        dataset_abstract = self.get_named_range_cell_value('DatasetAbstract')

        params = {
            'DataSetUUID': dataset_uuid,
            'DataSetTypeCV': dataset_type,
            'DataSetCode': dataset_code,
            'DataSetTitle': dataset_title,
            'DataSetAbstract': dataset_abstract
        }

        self.data_set = self.get_or_create(DataSets, params, filter_by=['DataSetCode'])

    def parse_analysis_results(self):
        """
        Parses rows from the 'DataColumns' table in the 'Analysis_Results' worksheet.
        :return: None
        """

        # Keep a reference to affiliations, sampling features, and methods to reduce db queries
        affiliations = defaultdict(lambda: None)  # type: dict[str, Affiliations]
        methods = defaultdict(lambda: None)
        sampling_features = defaultdict(lambda: None)
        collection_actions = defaultdict(lambda: None)

        table = self.tables.get('DataColumns', DataFrame())

        # Force values in 'Specimen Code' column to be strings
        table['Specimen Code'] = table['Specimen Code'].astype(str)

        for index, row in table.iterrows():
            # Get the Methods object that is needed to create the Actions object.
            # If the method does not exist in the database, skip inserting this row
            # as the method is required.
            method_code = row.get('Analysis Method Code')
            if method_code not in methods:
                try:
                    methods[method_code] = self.session.query(Methods).filter_by(MethodCode=method_code).one()
                except NoResultFound:
                    print('Skipped row {} in DataColumns table in Anaylsis_Results worksheet: Method "{}" not found.'.format(
                        index,
                        method_code
                    ))

                    continue

            # Get the sampling feature, which should already be parsed and
            # exist in the database. If not, then skip this row.
            sampling_feature_code = row.get('Specimen Code', '')
            if sampling_feature_code not in sampling_features:
                try:
                    sampling_features[sampling_feature_code] = self.session.query(SamplingFeatures)\
                        .filter_by(SamplingFeatureCode=sampling_feature_code)\
                        .one()
                except NoResultFound:
                    print('Skipped row {} in DataColumns table in Anaylsis_Results worksheet: Specimen "{}" not found.'.format(
                        index,
                        sampling_feature_code
                    ))

                    continue

            # Create the Actions object
            action = self.create(Actions, commit=False, **{
                'MethodObj': methods.get(method_code),
                'ActionTypeCV': 'Specimen analysis',
                'BeginDateTime': row.get('Analysis DateTime'),
                'BeginDateTimeUTCOffset': row.get('UTC Offset')
            })

            # Creat the FeatureActions object
            feature_action = self.create(FeatureActions, commit=False, **{
                'SamplingFeatureObj': sampling_features.get(sampling_feature_code),
                'ActionObj': action
            })

            # Get the Affiliations object for ActionBy
            analyst_name = row.get('Analyst Name', '')
            if analyst_name not in affiliations:
                names = self.parse_name(analyst_name)
                affiliations[analyst_name] = self.session.query(Affiliations) \
                    .join(People) \
                    .filter(People.PersonLastName == names.get('last_name', '')) \
                    .filter(People.PersonFirstName == names.get('first_name', '')) \
                    .filter(People.PersonMiddleName == names.get('middle_name', '')) \
                    .first()

            # Create the ActionBy object
            _ = self.create(ActionBy, commit=False, **{
                'IsActionLead': True,
                'AffiliationObj': affiliations[analyst_name],
                'ActionObj': action,
            })

            # Get the collection Actions object and create RelatedActions object
            specimen_code = row.get('Specimen Code')
            if specimen_code not in collection_actions:
                collection_actions[specimen_code] = self.session.query(FeatureActions) \
                    .filter(FeatureActions.FeatureActionID == SamplingFeatures.SamplingFeatureID) \
                    .filter(SamplingFeatures.SamplingFeatureCode == row.get('Specimen Code')) \
                    .first()

            assert(collection_actions[specimen_code] is not None)

            _ = self.create(RelatedActions, commit=False, **{
                'ActionObj': action,
                'RelationshipTypeCV': 'Is child of',
                'RelatedActionObj': collection_actions[specimen_code].ActionObj,
            })

            # Get the Variables, Units, and ProcessingLevels objects, which are
            # needed to create a MeasurementResults
            variable = self.get(Variables, VariableCode=row.get('Variable Code', ''))
            unit = self.get(Units, UnitsName=row.get('Units', ''))
            processing_lvl = self.get(ProcessingLevels, ProcessingLevelCode=row.get('Processing Level', ''))
            time_aggregation_unit = self.get(Units, UnitsName=row.get('Time Aggregation Unit', ''))

            if not all([variable, unit, processing_lvl, time_aggregation_unit]):
                print('Skipped row {} in DataColumns table in Anaylsis_Results worksheet because it contains missing or invalid data.'.format(index))

            # Create the MeasurementResults object
            result = self.create(MeasurementResults, commit=False, **{
                # 'ResultUUID': row.get('ResultUUID'),
                'ResultUUID': str(uuid4()),
                'CensorCodeCV': row.get('Censor Code CV'),
                'QualityCodeCV': row.get('Quality Code CV'),
                'TimeAggregationInterval': row.get('Time Aggregation Interval'),
                'TimeAggregationIntervalUnitsObj': time_aggregation_unit,
                'AggregationStatisticCV': row.get('Aggregation Statistic CV'),
                'FeatureActionObj': feature_action,
                'ResultTypeCV': 'Measurement',
                'VariableObj': variable,
                'UnitsObj': unit,
                'ProcessingLevelObj': processing_lvl,
                'StatusCV': 'Complete',
                'SampledMediumCV': row.get('Sampled Medium CV'),
                'ValueCount': 1,
                'ResultDateTime': collection_actions[specimen_code].ActionObj.BeginDateTime,
            })

            # Create MeasurementResultValues object
            _ = self.create(MeasurementResultValues, commit=False, **{
                'DataValue': row.get('Data Value'),
                'ValueDateTime': collection_actions[specimen_code].ActionObj.BeginDateTime,
                'ValueDateTimeUTCOffset': collection_actions[specimen_code].ActionObj.BeginDateTimeUTCOffset,
                'ResultObj': result
            })

            # Create DataSetsResults object
            _ = self.create(DataSetsResults, commit=False, **{
                'DataSetObj': self.data_set,
                'ResultObj': result
            })

            self._updateGauge()

        self.session.commit()

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

        self._updateGauge(table.shape[0])

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

        self._updateGauge(organization_table.shape[0])

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

        self._updateGauge(people_table.shape[0])

    def get_sheet_and_table(self, sheet_name):
        """
        Leaving method here for reference. It's used by another method
        that isn't being used (parse_spatial_reference()).
        """
        # sheet = self.workbook.get_sheet_by_name(sheet_name)
        # return sheet, self.tables.get(sheet_name, [])
        return NotImplementedError

    def parse_processing_level(self):

        table = self.tables.get('ProcessingLevels', DataFrame())
        for _, row in table.iterrows():

            params = {
                'ProcessingLevelCode': row.get('Processing Level Code'),
                'Definition': row.get('Definition'),
                'Explanation': row.get('Explanation')
            }

            _ = self.get_or_create(ProcessingLevels, params, filter_by=['ProcessingLevelCode'])

        self._updateGauge(table.shape[0])

    def parse_sampling_feature(self):

        elevation_datum = self.get_named_range_cell_value('ElevationDatum')
        latlon_datum = self.get_named_range_cell_value('LatLonDatum')

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

        self._updateGauge(table.shape[0])

    def parse_spatial_reference(self):
        """
        Keeping this here for reference (no pun intended), but the commented out code
        is not a correct implemenation, nor does a worksheet named "SpatialReferences"
        exist in the SpecimenTimeSeries excel YODA file...
        :return:
        """
        # SHEET_NAME = "SpatialReferences"
        # sheet, tables = self.get_sheet_and_table(SHEET_NAME)
        #
        # if not len(tables):
        #     return []
        #
        # spatial_references = {}
        # for table in tables:
        #     cells = sheet[self.get_range_address(table)]
        #     for row in cells:
        #         sr = SpatialReferences()
        #         sr.SRSCode = row[0].value
        #         sr.SRSName = row[1].value
        #         sr.SRSDescription = row[2].value
        #         sr.SRSLink = row[3].value
        #
        #         spatial_references[sr.SRSName] = sr
        #
        # return spatial_references
        raise NotImplementedError

    def parse_specimens(self):
        """
        Parse rows in the 'Specimens' table on the 'Specimens' worksheet
        :return: None
        """

        # keep track of collection sites, methods to reduce number of db queries
        collection_sites = defaultdict(lambda: None)
        methods = defaultdict(lambda: None)

        table = self.tables.get('Specimens', DataFrame())

        # Force values in 'Sampling Feature Code' column to be strings
        table['Sampling Feature Code'] = table['Sampling Feature Code'].astype(str)

        for _, row in table.iterrows():

            # First get the sampling feature for the RelatedFeatures object that will
            # be created later. If the sampling feature does not exist in the database,
            # skip inserting this row, since the sampling feature (which should have
            # been parsed from the 'Sites' excel sheet) is required.
            collection_site_code = row.get('Collection Site', '')
            if collection_site_code not in collection_sites:
                try:
                    collection_sites[collection_site_code] = self.session.query(SamplingFeatures).filter(
                        SamplingFeatures.SamplingFeatureCode == collection_site_code
                    ).one()
                except NoResultFound:
                    print('Error: Collection Site "{}" not found. Skipping database insertion of Specimen "{}".'.format(
                        collection_site_code,
                        row.get('Sampling Feature Code')
                    ))

                    continue

            # Next, get the Methods object for the Actions object that will also be
            # created later. Once again, if the method does not exist in the database,
            # skip inserting this row since the method is required.
            method_code = row.get('Collection Method Code', '')
            if method_code not in methods:
                try:
                    methods[method_code] = self.session.query(Methods) \
                        .filter(Methods.MethodCode == method_code) \
                        .one()
                except NoResultFound:
                    print('Error: Method "{}" not found. Skipping database insertion of Specimen "{}"'.format(
                        method_code,
                        row.get('Sampling Feature Code')
                    ))

                    continue

            # Finally, create the SamplingFeatures specimen object for this row.
            params = {
                # 'SamplingFeatureUUID': row.get('Sampling Feature UUID'),
                'SamplingFeatureUUID': str(uuid4()),
                'SamplingFeatureCode': row.get('Sampling Feature Code'),
                'SamplingFeatureName': row.get('Sampling Feature Name'),
                'SamplingFeatureDescription': row.get('Sampling Feature Description'),
                'SamplingFeatureTypeCV': 'Specimen',
                'SpecimenMediumCV': row.get('Specimen Medium [CV]'),
                'IsFieldSpecimen': row.get('Is Field Specimen?'),
                'ElevationDatumCV': 'Unknown',
                'SpecimenTypeCV': row.get('Specimen Type [CV]')
            }

            sampling_feature = self.get_or_create(Specimens, params, filter_by=['SamplingFeatureCode'], commit=False)

            # Create the RelatedFeatures object.
            _ = self.create(RelatedFeatures, commit=False, **{
                'RelationshipTypeCV': 'Was Collected at',
                'SamplingFeatureObj': sampling_feature,
                'RelatedFeatureObj': collection_sites[collection_site_code],
            })

            # Create the Actions object
            action = self.create(Actions, commit=False, **{
                'ActionTypeCV': 'Specimen collection',
                'BeginDateTime': row.get('Collection Date Time'),
                'BeginDateTimeUTCOffset': row.get('UTC Offset'),
                'MethodObj': methods.get(method_code)
            })

            # And finally, create the FeatureActions object
            _ = self.create(FeatureActions, commit=False, **{
                'ActionObj': action,
                'SamplingFeatureObj': sampling_feature
            })

            self._updateGauge()

        self.session.commit()

    def parse_methods(self):
        """
        Parse Methods recorded in the excel template

        NOTE: When parsing SpecimenTimeSeries templates, there are two
        seperate tables - the SpecimenCollectionMethods table, and the
        SpecimenAnalysisMethods table. `parse_methods()` parses both tables.
        :return: None
        """

        collections_method_table = self.tables.get('SpecimenCollectionMethods')
        analysis_methods_table = self.tables.get('SpecimenAnalysisMethods')
        table = collections_method_table.append(analysis_methods_table)  # type: DataFrame

        # Force values in 'Method Code' column to be strings
        table['Method Code'] = table['Method Code'].astype(str)

        for _, row in table.iterrows():

            _ = self.parse_method(**row)

        self._updateGauge(table.shape[0])

    def parse_method(self, **kwargs):

        org = self._orgs.get(kwargs.get('Organization Name'))

        params = {
            'MethodTypeCV': kwargs.get('Method Type [CV]'),
            'MethodCode': kwargs.get('Method Code'),
            'MethodName': kwargs.get('Method Name'),
            'OrganizationObj': org
        }

        # check if params has required fields
        assert all(params.values()), 'Values = %s ' % str(params.values())

        # After checking for required fields, add the non required field
        params.update(MethodLink=kwargs.get('MethodLink'), MethodDescription=kwargs.get('Method Description'))

        return self.get_or_create(Methods, params, filter_by='MethodCode')


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

        self._updateGauge(table.shape[0])
