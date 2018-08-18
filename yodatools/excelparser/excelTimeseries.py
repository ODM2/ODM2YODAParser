import os
import time
from datetime import datetime
import string
from collections import defaultdict
from uuid import uuid4

import pandas as pd
from pandas import DataFrame
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from openpyxl.worksheet.worksheet import Worksheet

from .ExcelParser import ExcelParser
from odm2api.models import \
    (DataSets,
     Citations,
     AuthorLists,
     People,
     Units,
     SamplingFeatures,
     Organizations,
     Affiliations,
     ProcessingLevels,
     Sites,
     SpatialReferences,
     Methods,
     Variables,
     Actions,
     FeatureActions,
     ActionBy,
     TimeSeriesResults,
     DataSetsResults,
     TimeSeriesResultValues,
     CVUnitsType,
     CVVariableName,
     setSchema)







class ExcelTimeseries(ExcelParser):

    # https://automatetheboringstuff.com/chapter12/
    def __init__(self, input_file, session_factory, **kwargs):
        super(ExcelTimeseries, self).__init__(input_file, session_factory, **kwargs)

        self.sampling_features = defaultdict(lambda: None)

    def _init_data(self, file_path):
        """
        Reads the "Data Values" worksheet into a DataFrame and adds it to `self.tables`.

        Values in the "Data Values" worksheet are not formatted as an excel table because
        users can add any number or combination of column names they deem necessary,
        making it basically impossible to create a standardized template.

        See https://github.com/ODM2/YODA-File/tree/master/examples/time_series for examples.
        """
        super(ExcelTimeseries, self)._init_data(file_path)

        sheet = self.workbook.get_sheet_by_name('Data Values')  # type: Worksheet
        datavalue_generator = self.__generate_data_values(sheet.iter_rows())

        headers = next(datavalue_generator)

        self.tables['DataValues'] = DataFrame([dv for dv in datavalue_generator], columns=headers)

        self.workbook.close()


    def __generate_data_values(self, rows):
        for row in rows:
            dvals = [cell.value for cell in row]
            if not any(dvals):
                return
            yield dvals

    def get_table_name_ranges(self):
        """
        Returns a list of the name range that have a table.
        The name range should contain the cells locations of the data.
        :rtype: list
        """
        CONST_NAME = "_Table"
        table_name_range = {}
        for name_range in self.name_ranges:
            if CONST_NAME in name_range.name:
                sheet = name_range.attr_text.split('!')[0]
                sheet = sheet.replace('\'', '')

                if sheet in table_name_range:
                    table_name_range[sheet].append(name_range)
                else:
                    table_name_range[sheet] = [name_range]

        return table_name_range

    def get_range_address(self, named_range):
        """
        Depracated

        :param named_range:
        :return:
        """
        if named_range is not None:
            return named_range.attr_text.split('!')[1].replace('$', '')
        return None

    def get_range_value(self, range_name, sheet):
        """
        Depracated

        :param range_name:
        :param sheet:
        :return:
        """
        value = None
        named_range = self.workbook.get_named_range(range_name)
        range_ = self.get_range_address(named_range)
        if range_:
            value = sheet[range_].value
        return value

    def get_sheet_and_table(self, sheet_name):
        """
        Depracated

        :param sheet_name:
        :return:
        """
        if sheet_name not in self.tables:
            return [], []
        sheet = self.workbook.get_sheet_by_name(sheet_name)
        tables = self.tables[sheet_name]

        return sheet, tables

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
        self.parse_sampling_features()

        # self.parse_specimens()
        # self.parse_analysis_results()
        self.parse_data_columns()

        end = time.time()

        hours, remainder = divmod(end - start, 3600)
        minutes, seconds = divmod(remainder, 60)

        self.update_progress_label('Input completed in %s:%s:%s' % (int(hours), int(minutes), int(seconds)))

    def parse_sampling_features(self):

        self.update_progress_label('Reading Sampling Features table')

        elevation_datum = self.get_named_range_cell_value('ElevationDatum')
        latlon_datum = self.get_named_range_cell_value('LatLonDatum')
        spatial_ref = self.spatial_references.get(latlon_datum.lower(), None)

        table = self.tables.get('SamplingFeatures', DataFrame())
        for _, row in table.iterrows():

            params = {
                'SamplingFeatureUUID': row.get('Sampling Feature UUID', str(uuid4())),
                'SamplingFeatureTypeCV': row.get('Sampling Feature Type'),
                'SamplingFeatureCode': row.get('Feature Code'),
                'SiteTypeCV': row.get('Site Type'),
                'Latitude': row.get('Latitude'),
                'Longitude': row.get('Longitude'),
                'SpatialReferenceObj': spatial_ref
            }

            assert(all(params.values()))

            params.update({
                'ElevationDatumCV': elevation_datum,
                'SamplingFeatureName': row.get('Feature Name'),
                'SamplingFeatureDescription': row.get('Feature Description'),
                'FeatureGeometryWKT': row.get('Feature Geometry'),
                'Elevation_m': row.get('Elevation_m'),
                'SamplingFeatureGeotypeCV': row.get('Feature Geo Type')
            })

            sf = self.get_or_create(Sites, params, filter_by=['SamplingFeatureCode'], commit=False)
            self.sampling_features[params.get('SamplingFeatureCode').lower()] = sf

        self.session.commit()

        self._updateGauge(table.shape[0])



    def is_valid(self, iterable):
        for element in iterable:
            if element.value is None:
                return False
        return True


    def parse_data_values(self):
        print "working on datavalues"
        CONST_COLUMNS = "Data Columns"
        if CONST_COLUMNS not in self.tables:
            print "No Variables found"
            return []

        sheet = self.workbook.get_sheet_by_name(CONST_COLUMNS)
        tables = self.tables[CONST_COLUMNS]

        data_values = pd.read_excel(io=self.input_file, sheetname='Data Values')
        start_date = data_values["LocalDateTime"].iloc[0].to_pydatetime()
        end_date = data_values["LocalDateTime"].iloc[-1].to_pydatetime()
        utc_offset = int(data_values["UTCOffset"][0])
        value_count = len(data_values.index)

        metadata = {}

        for table in tables:
            cells = sheet[self.get_range_address(table)]

            print "looping through datavalues"
            for row in cells:
                if self.is_valid(row):

                    action = Actions()
                    feat_act = FeatureActions()
                    act_by = ActionBy()
                    series_result = TimeSeriesResults()
                    dataset_result = DataSetsResults()


                    # Action
                    method = self.session.query(Methods).filter_by(MethodCode=row[4].value).first()
                    action.MethodObj = method
                    #TODO ActionType
                    action.ActionTypeCV = "Observation"
                    action.BeginDateTime = start_date
                    action.BeginDateTimeUTCOffset = utc_offset
                    action.EndDateTime = end_date
                    action.EndDateTimeUTCOffset = utc_offset

                    # Feature Actions
                    sampling_feature = self.session.query(SamplingFeatures)\
                        .filter_by(SamplingFeatureCode=row[3].value)\
                        .first()

                    feat_act.SamplingFeatureObj = sampling_feature
                    feat_act.ActionObj = action

                    # Action By
                    names = filter(None, row[5].value.split(' '))
                    if len(names) > 2:
                        last_name = names[2].strip()
                    else:
                        last_name = names[1].strip()
                    first_name = names[0].strip()

                    person = self.session.query(People).filter_by(PersonLastName=last_name, PersonFirstName=first_name).first()
                    affiliations = self.session.query(Affiliations).filter_by(PersonID=person.PersonID).first()
                    act_by.AffiliationObj = affiliations
                    act_by.ActionObj = action
                    act_by.IsActionLead = True


                    # self.session.no_autoflush
                    self.session.flush()

                    self.session.add(action)
                    self.session.flush()
                    self.session.add(feat_act)
                    self.session.add(act_by)
                    # self.session.add(related_action)
                    self.session.flush()
                    # Measurement Result (Different from Measurement Result Value) also creates a Result
                    variable = self.session.query(Variables).filter_by(VariableCode=row[7].value).first()


                    units_for_result = self.session.query(Units).filter_by(UnitsName=row[8].value).first()
                    proc_level = self.session.query(ProcessingLevels).filter_by(ProcessingLevelCode=str(row[9].value)).first()

                    units_for_agg = self.session.query(Units).filter_by(UnitsName=row[12].value).first()

                    # series_result.IntendedTimeSpacing = row[11].value
                    # series_result.IntendedTimeSpacingUnitsObj = units_for_agg
                    series_result.AggregationStatisticCV = row[13].value
                    series_result.ResultUUID = row[2].value
                    series_result.FeatureActionObj = feat_act
                    series_result.ResultTypeCV = row[6].value
                    series_result.VariableObj = variable
                    series_result.UnitsObj = units_for_result
                    series_result.ProcessingLevelObj = proc_level

                    series_result.StatusCV = "Unknown"
                    series_result.SampledMediumCV = row[10].value
                    series_result.ValueCount = value_count

                    series_result.ResultDateTime = start_date

                    self.session.add(series_result)
                    # self.session.flush()  # steph
                    self.session.commit()  # me

                    if self.dataset is not None:
                        #DataSetsResults
                        dataset_result.DataSetObj = self.dataset
                        dataset_result.ResultObj = series_result
                        self.session.add(dataset_result)

                    # Timeseries Result Value Metadata

                    metadata[row[1].value] = {
                        'Result': series_result,
                        'CensorCodeCV': row[14].value,
                        'QualityCodeCV': row[15].value,
                        'TimeAggregationInterval': row[11].value,
                        'TimeAggregationIntervalUnitsObj': units_for_agg
                    }

                    # self.session.add(measure_result_value)
                    self._flush()

                    self._updateGauge()

        print "convert from cross tab to serial"
        return self.load_time_series_values(data_values, metadata)

    def parse_data_columns(self):
        """
        Parses the 'DataColumns' table and 'Data Values' worksheet from a TimeSeries excel file.

        Each row in 'DataColumns' corresponds to a TimeSeriesResults object, and each
        column in 'Data Values' coresponds to several TimeSeriesResultValues objects.
        Reference `http://odm2.github.io/ODM2/schemas/ODM2_Current/diagrams/ODM2Results.html`
        for a visual of the database schema.

        :return:
        """
        self.update_progress_label('Reading DataColumns table')

        affiliations = defaultdict(lambda: None)

        datacolumns = self.tables.get('DataColumns', DataFrame())
        datavalues = self.tables.get('DataValues', DataFrame())

        datetimes = datavalues.get('LocalDateTime').dt.to_pydatetime()
        startdate = min(*datetimes)
        enddate = max(*datetimes)
        utcoffset = datavalues.get('UTCOffset').pop(0)
        value_count = len(datavalues.index)

        for index, row in datacolumns.iterrows():

            # TODO: check that method exists
            methcode = row.get('Method Code').lower()
            method = self.methods.get(methcode)

            # TODO: Check that sampling_feature exists
            sfcode = row.get('Sampling Feature Code').lower()
            sampling_feature = self.sampling_features.get(sfcode)

            action = self.create_action(start_date=startdate,  # type: Actions
                                        end_date=enddate,
                                        utcoffset=utcoffset,
                                        method=method)

            ftraction = self.create_feature_action(sampling_feature=sampling_feature,  # type: FeatureActions
                                                   action=action)

            # get or create the Affiliations object
            fullname = row.get('Data Collector')
            if fullname not in affiliations:
                names = self.parse_name(fullname)
                affiliations[fullname] = self.session.query(Affiliations) \
                    .join(People) \
                    .filter(People.PersonLastName == names.get('last_name', '')) \
                    .filter(People.PersonFirstName == names.get('first_name', '')) \
                    .filter(People.PersonMiddleName == names.get('middle_name', '')) \
                    .first()

            actionby = self.create_action_by(affiliation=affiliations.get(fullname), action=action)

            variable = self.variables.get(row.get('Variable Code').lower())
            unit = self.units.get(row.get('Unit Name').lower())
            processing_lvl = self.processing_levels.get(row.get('Processing Level'))
            aggregation_unit = self.units.get(row.get('Time Aggregation Unit').lower())

            if not all([variable, unit, processing_lvl, aggregation_unit]):
                self.update_output_text('Skipped row {} in DataColumns table in Anaylsis_Results worksheet because it contains missing or invalid data.'.format(index + 1))

            result = self.create(MeasurementResults, commit=False, **{
                'AggregationStatisticCV': row.get('Aggregation Statistic'),
                'ResultUUID': row.get('ResultUUID', str(uuid4())),
                'FeatureActionObj': ftraction,
                'ResultTypeCV': row.get('Result Type'),
                'VariableObj': variable,
                'UnitsObj': unit,
                'ProcessingLevelObj': processing_lvl,
                'StatusCV': "Unknown",
                'SampledMediumCV': row.get('Sampled Medium'),
                'ValueCount': len(row.index),
                'ResultDateTime': startdate
            })

            # Create DataSetsResults object
            _ = self.create(DataSetsResults, commit=False, **{
                'DataSetObj': self.data_set,
                'ResultObj': result
            })

            self._updateGauge()

            # TODO: This is where you left off.
            # Things to do:
            #   1. see if this method works up to this point
            #   2. do the magic of parsing datavalues
            #   3. document stuff

            dvcol = datavalues.get(row.get('Column Label'))

    def parse_datavalues(self, datavalues):
        pass


    def create_action(self, start_date, end_date, utcoffset, method):  # type: (datetime, datetime, int, Methods) -> Actions
        return self.create(Actions, commit=False, **{
            'MethodObj': method,
            'ActionTypeCV': "Observation",
            'BeginDateTime': start_date,
            'BeginDateTimeUTCOffset': utcoffset,
            'EndDateTime': end_date,
            'EndDateTimeUTCOffset': utcoffset
        })

    def create_feature_action(self, sampling_feature, action):  # type: (SamplingFeatures, Actions) -> FeatureActions
        return self.create(FeatureActions, commit=False, **{
            'SamplingFeatureObj': sampling_feature,
            'ActionObj': action
        })

    def create_action_by(self, affiliation, action):  # type: (Affiliations, Actions) -> ActionBy
        return self.create(ActionBy, commit=False, **{
            'AffiliationObj': affiliation,
            'ActionObj': action,
            'IsActionLead': True
        })

    def load_time_series_values(self, cross_tab, meta_dict):
        """
        Loads TimeSeriesResultsValues into pandas DataFrame
        """

        date_column = "LocalDateTime"
        utc_column = "UTCOffset"

        cross_tab.set_index([date_column, utc_column], inplace=True)

        serial = cross_tab.unstack(level=[date_column, utc_column])

        # add all the columns we need and clean up the dataframe
        serial = serial.append(
            pd.DataFrame(columns=['ResultID', 'CensorCodeCV', 'QualityCodeCV', 'TimeAggregationInterval',
                                  'TimeAggregationIntervalUnitsID'])) \
            .fillna(0) \
            .reset_index() \
            .rename(columns={0: 'DataValue'}) \
            .rename(columns={date_column: 'ValueDateTime', 'UTCOffset': 'ValueDateTimeUTCOffset'}) \
            .dropna()

        for k, v in meta_dict.iteritems():
            serial.ix[serial.level_0 == k, 'ResultID'] = v["Result"].ResultID
            serial.ix[serial.level_0 == k, 'CensorCodeCV'] = v["CensorCodeCV"]
            serial.ix[serial.level_0 == k, 'QualityCodeCV'] = v["QualityCodeCV"]
            serial.ix[serial.level_0 == k, 'TimeAggregationInterval'] = v["TimeAggregationInterval"]
            serial.ix[serial.level_0 == k, 'TimeAggregationIntervalUnitsID'] = v["TimeAggregationIntervalUnitsObj"].UnitsID

        del serial['level_0']


        if ':memory:' not in repr(self._engine):
            # ':memory:' is part of the session engine connection string
            # when using sqlite in-memory storage (as opposed to a file).
            # If the session engine is connected to a database NOT stored
            # in memory, the connection must be closed before 'serial.to_sql'
            # can connect to the database.
            self.session.close()

        if 'postgresql' in repr(self._engine):
            # Column names are lower cased when using postgresql...
            serial.columns = map(str.lower, serial.columns)

        setSchema(self._engine)
        serial.to_sql(TimeSeriesResultValues.__tablename__,
                      schema=TimeSeriesResultValues.__table_args__['schema'],
                      if_exists='append',
                      chunksize=1000,
                      con=self._engine,
                      index=False)

        self.session.flush()

        return serial
