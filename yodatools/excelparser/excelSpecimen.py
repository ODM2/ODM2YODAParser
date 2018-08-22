import os
from datetime import timedelta
from collections import defaultdict
from uuid import uuid4
import time
import string
import re

import pandas as pd
from pandas import DataFrame
import numpy as np
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
import openpyxl
from openpyxl.worksheet.table import Table
from openpyxl.workbook.workbook import Workbook
from openpyxl.cell.cell import Cell
from pubsub import pub

from odm2api.models import *
from yodatools.converter.Abstract import iInputs

from .excelParser import ExcelParser


class ExcelSpecimen(ExcelParser):
    def __init__(self, input_file, session_factory, **kwargs):

        super(ExcelSpecimen, self).__init__(input_file, session_factory, **kwargs)

        self.sites = defaultdict(lambda: None)

    def _init_data(self, file_path):
        super(ExcelSpecimen, self)._init_data(file_path)
        self.workbook.close()

    def parse(self):
        """
        Parses the excel file read in self._init_data
        :return: None
        """

        start = time.time()

        self.parse_people_and_orgs()
        self.parse_datasets()
        self.parse_methods()
        self.parse_variables()
        self.parse_units()
        self.parse_processing_level()
        self.parse_spatial_reference()
        self.parse_sites()
        self.parse_specimens()
        self.parse_analysis_results()


        end = time.time()

        hours, remainder = divmod(end - start, 3600)
        minutes, seconds = divmod(remainder, 60)

        self.update_progress_label('Input completed in %s:%s:%s' % (int(hours), int(minutes), int(seconds)))

        return True

    def parse_analysis_results(self):
        """
        Parses rows from the 'DataColumns' table in the 'Analysis_Results' worksheet.
        :return: None
        """

        sampling_features = defaultdict(lambda: None)
        collection_actions = defaultdict(lambda: None)

        table = self.tables.get('Analysis_Results', DataFrame())

        # Force values in 'Specimen Code' column to be strings
        table['Specimen Code'] = table['Specimen Code'].astype(str)

        row_count, _ = table.shape

        for index, row in table.iterrows():

            self.update_progress_label('Reading Analysis_Results table %d/%d' % (index + 1, row_count))

            # Get the Methods object that is needed to create the Actions object.
            # If the method does not exist in the database, skip inserting this row
            # as the method is required.
            method_code = row.get('Analysis Method Code')
            if method_code.lower() not in self.methods:
                self.update_output_text('Skipped \'Anaylsis_Results\':\'Anaylsis_Results\' row {} - Method "{}" not found'.format(
                    index,
                    method_code
                ))

                continue

            method = self.methods.get(method_code.lower())

            # Get the sampling feature, which should already be parsed and
            # exist in the database. If not, then skip this row.
            sampling_feature_code = row.get('Specimen Code', '')
            if sampling_feature_code not in sampling_features:
                try:
                    sampling_features[sampling_feature_code] = self.session.query(SamplingFeatures)\
                        .filter_by(SamplingFeatureCode=sampling_feature_code)\
                        .one()
                except NoResultFound:
                    self.update_output_text("Skipped 'Analysis_Results':'Analysis_Results' row {} - Sampling Feature Code '{}' did not map to any Specimens.".format(
                        index + 1,
                        sampling_feature_code
                    ))

                    continue

            # Create the Actions object
            action = self.create(Actions, commit=False, **{
                'MethodObj': method,
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
            # if analyst_name not in affiliations:
            #     names = self.parse_name(analyst_name)
            #     affiliations[analyst_name] = self.session.query(Affiliations) \
            #         .join(People) \
            #         .filter(People.PersonLastName == names.get('last', '')) \
            #         .filter(People.PersonFirstName == names.get('first', '')) \
            #         .filter(People.PersonMiddleName == names.get('middle', '')) \
            #         .first()

            # Create the ActionBy object
            _ = self.create(ActionBy, commit=False, **{
                'IsActionLead': True,
                'AffiliationObj': self.affiliations.get(analyst_name),
                'ActionObj': action,
            })

            # Get the collection Actions object and create RelatedActions object
            specimen_code = row.get('Specimen Code')
            if specimen_code not in collection_actions:
                collection_actions[specimen_code] = self.session.query(FeatureActions) \
                    .filter(FeatureActions.FeatureActionID == SamplingFeatures.SamplingFeatureID) \
                    .filter(SamplingFeatures.SamplingFeatureCode == row.get('Specimen Code')) \
                    .first()

            if collection_actions[specimen_code] is None:
                self.update_output_text("Skipped 'AnalysisResults':'AnalysisResults' row %d - FeatureAction with Sampling Feature Code '%s' not found" % (index + 1, specimen_code))
                continue

            _ = self.create(RelatedActions, commit=False, **{
                'ActionObj': action,
                'RelationshipTypeCV': 'Is child of',
                'RelatedActionObj': collection_actions[specimen_code].ActionObj,
            })

            # Get the Variables, Units, and ProcessingLevels objects, which are
            # needed to create a MeasurementResults
            variable = self.variables.get(row.get('Variable Code').lower())
            unit = self.units.get(row.get('Units').lower())
            processing_lvl = self.get(ProcessingLevels, ProcessingLevelCode=row.get('Processing Level', ''))
            time_aggregation_unit = self.get(Units, UnitsName=row.get('Time Aggregation Unit', ''))

            if not all([variable, unit, processing_lvl, time_aggregation_unit]):
                self.update_output_text('Skipped row {} in DataColumns table in Anaylsis_Results worksheet because it contains missing or invalid data.'.format(index + 1))

            # Create the MeasurementResults object
            result = self.create(MeasurementResults, commit=False, **{
                'ResultUUID': row.get('ResultUUID', str(uuid4())),
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

    def parse_sites(self):

        elevation_datum = self.get_named_range_cell_value('ElevationDatum')

        latlon_datum = self.get_named_range_cell_value('LatLonDatum')
        spatial_ref = self.spatial_references.get(latlon_datum.lower(), None)

        table = self.tables.get('Sites', DataFrame())

        self.update_progress_label('Reading Sites table')

        for _, row in table.iterrows():

            params = {
                'SamplingFeatureUUID': row.get('Sampling Feature UUID', str(uuid4())),
                'SamplingFeatureCode': row.get('Sampling Feature Code'),
                'SamplingFeatureTypeCV': 'Site',
                'SiteTypeCV': row.get('Site Type'),
                'Latitude': row.get('Latitude'),
                'Longitude': row.get('Longitude'),
                'SpatialReferenceObj': spatial_ref
            }

            assert (all(params.values()))

            params.update({
                'SamplingFeatureName': row.get('Sampling Feature Name'),
                'SamplingFeatureDescription': row.get('Sampling Feature Description'),
                'FeatureGeometryWKT': row.get('Feature Geometry WKT'),
                'Elevation_m': row.get('Elevation_m'),
                'ElevationDatumCV': elevation_datum,
            })

            self.sites[params.get('SamplingFeatureCode').lower()] = self.get_or_create(Sites, params, filter_by=['SamplingFeatureCode'], commit=False)

        self.session.commit()

        self._updateGauge(table.shape[0])

    def parse_specimens(self):
        """
        Parse rows in the 'Specimens' table on the 'Specimens' worksheet
        :return: None
        """

        table = self.tables.get('Specimens', DataFrame())

        # Force values in 'Sampling Feature Code' column to be strings
        table['Sampling Feature Code'] = table['Sampling Feature Code'].astype(str)

        row_count, _ = table.shape

        for index, row in table.iterrows():

            self.update_progress_label('Reading Specimens table %d/%d' % (index + 1, row_count))

            # First get the sampling feature for the RelatedFeatures object that will
            # be created later. If the sampling feature does not exist in the database,
            # skip inserting this row, since the sampling feature (which should have
            # been parsed from the 'Sites' excel sheet) is required.
            collection_site_code = row.get('Collection Site', '')
            if collection_site_code.lower() not in self.sites:
                self.update_output_text('Error: Collection Site "{}" not found. Skipping database insertion of Specimen "{}".'.format(
                    collection_site_code,
                    row.get('Sampling Feature Code')
                ))

                continue

            collection_site = self.sites.get(collection_site_code.lower(), None)

            # Next, get the Methods object for the Actions object that will also be
            # created later. Once again, if the method does not exist in the database,
            # skip inserting this row since the method is required.
            method_code = row.get('Collection Method Code', '')
            if method_code.lower() not in self.methods:
                self.update_output_text('Error: Method "{}" not found. Skipping database insertion of Specimen "{}"'.format(
                    method_code,
                    row.get('Sampling Feature Code')
                ))

                continue

            method = self.methods.get(method_code.lower())

            # Finally, create the SamplingFeatures specimen object for this row.
            params = {
                'SamplingFeatureUUID': row.get('Sampling Feature UUID'),
                'SamplingFeatureCode': row.get('Sampling Feature Code'),
                'SamplingFeatureName': row.get('Sampling Feature Name'),
                'SamplingFeatureDescription': row.get('Sampling Feature Description'),
                'SamplingFeatureTypeCV': 'Specimen',
                'SpecimenMediumCV': row.get('Specimen Medium'),
                'IsFieldSpecimen': row.get('Is Field Specimen?'),
                'ElevationDatumCV': 'Unknown',
                'SpecimenTypeCV': row.get('Specimen Type')
            }

            sampling_feature = self.get_or_create(Specimens, params, filter_by=['SamplingFeatureCode'], commit=False)

            # Create the RelatedFeatures object.
            _ = self.create(RelatedFeatures, commit=False, **{
                'RelationshipTypeCV': 'Was Collected at',
                'SamplingFeatureObj': sampling_feature,
                'RelatedFeatureObj': collection_site
            })

            # Create the Actions object
            action = self.create(Actions, commit=False, **{
                'ActionTypeCV': 'Specimen collection',
                'BeginDateTime': row.get('Collection Date Time'),
                'BeginDateTimeUTCOffset': row.get('UTC Offset'),
                'MethodObj': method
            })

            # And finally, create the FeatureActions object
            _ = self.create(FeatureActions, commit=False, **{
                'ActionObj': action,
                'SamplingFeatureObj': sampling_feature
            })

            self._updateGauge()

        self.session.commit()

    def parse_methods(self, table=None):
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

        super(ExcelSpecimen, self).parse_methods(table=table)
