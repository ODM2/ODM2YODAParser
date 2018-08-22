import os
import time
from datetime import datetime
import string
from collections import defaultdict
from uuid import uuid4
# from threading import Thread
from multiprocessing.dummy import Pool

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm.exc import NoResultFound
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import Cell

from .excelParser import ExcelParser
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
        self.timeseriesresults = defaultdict(lambda: None)

    def _init_data(self, file_path):
        """
        Reads the "Data Values" worksheet into a DataFrame and adds it to `self.tables`.

        Values in the "Data Values" worksheet are not formatted as an excel table because
        users can add any number or combination of column names they deem necessary,
        making it basically impossible to create a standardized template.

        See https://github.com/ODM2/YODA-File/tree/master/examples/time_series for examples.
        """
        super(ExcelTimeseries, self)._init_data(file_path)
        self.__read_data_values()
        self.workbook.close()

    def __read_data_values(self):
        """
        Reads the `Data Values` worksheet in a Time-Series excel template.

        Because the `Data Values` worksheet can contain a huge amount of data,
        the worksheet is read and parsed row by row in `parse_data_values()`
        :return:
        """
        sheet = self.workbook.get_sheet_by_name('Data Values')  # type: Worksheet
        dvs = self.__dv_row_generator(sheet.iter_rows())
        headers = next(dvs)
        df = DataFrame([dv for dv in dvs], columns=headers)
        df.dropna(how='all', inplace=True)

        self.datavalue_count = df.shape[0]

        self.tables['DataValues'] = df

    def __dv_row_generator(self, rows):  # type: ([Cell]) -> generator
        """
        It... does... things...

        :param rows: a list of Cells
        :return: a generator object
        """
        for row in rows:
            values = [cell.value for cell in row]
            if not any(values):
                return
            yield values

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

        self._updateGauge(int(self.total_rows_to_read * 0.01))

        self.parse_people_and_orgs()
        self.parse_datasets()
        self.parse_methods()
        self.parse_variables()
        self.parse_units()
        self.parse_processing_level()
        self.parse_spatial_reference()
        self.parse_sampling_features()
        self.parse_data_columns()
        self.parse_datavalues()

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

    def parse_data_columns(self):
        """
        Parses the 'DataColumns' table and 'Data Values' worksheet from a TimeSeries excel file.

        Each row in 'DataColumns' corresponds to a TimeSeriesResults object, and each
        column in 'Data Values' coresponds to several TimeSeriesResultValues objects.
        Reference `http://odm2.github.io/ODM2/schemas/ODM2_Current/diagrams/ODM2Results.html`
        for a visual of the database schema.

        :return: None
        """
        self.update_progress_label(message='Reading DataColumns table')

        datacolumns = self.tables.get('DataColumns', DataFrame())
        datacolumns['Processing Level'] = datacolumns['Processing Level'].astype(int).astype(str)

        utcoffset = int(self.tables['DataValues'].get('UTCOffset')[0])
        datetimes = self.tables['DataValues'].get('LocalDateTime').dt.to_pydatetime()
        startdate = min(*datetimes)
        enddate = max(*datetimes)

        row_count, _ = datacolumns.shape

        for index, row in datacolumns.iterrows():

            self._updateGauge(1, message='Reading DataColumns table row %s of %s' % (index + 1, row_count))

            # TODO: check that `method` exists
            methcode = row.get('Method Code').lower()
            method = self.methods.get(methcode)

            # TODO: Check that `sampling_feature` exists
            sfcode = row.get('Sampling Feature Code').lower()
            sampling_feature = self.sampling_features.get(sfcode)

            action = self.create_action(start_date=startdate,  # type: Actions
                                        end_date=enddate,
                                        utcoffset=utcoffset,
                                        method=method)

            ftraction = self.create_feature_action(sampling_feature=sampling_feature,  # type: FeatureActions
                                                   action=action)

            # Create the ActionsBy object
            _ = self.create_action_by(affiliation=self.affiliations.get(row.get('Data Collector')),
                                      action=action)

            variable = self.variables.get(row.get('Variable Code').lower())
            unit = self.units.get(row.get('Unit Name').lower())
            processing_lvl = self.processing_levels.get(row.get('Processing Level'))
            aggregation_unit = self.units.get(row.get('Time Aggregation Unit').lower())

            if not all([variable, unit, processing_lvl, aggregation_unit]):
                self._updateGauge('Skipped row {} in DataColumns table in Anaylsis_Results worksheet because it contains missing or invalid data.'.format(index + 1))
                continue

            result = self.create(TimeSeriesResults, commit=False, **{
                'AggregationStatisticCV': row.get('Aggregation Statistic'),
                'ResultUUID': row.get('ResultUUID'),
                'FeatureActionObj': ftraction,
                'ResultTypeCV': row.get('Result Type'),
                'VariableObj': variable,
                'UnitsObj': unit,
                'ProcessingLevelObj': processing_lvl,
                'StatusCV': "Unknown",
                'SampledMediumCV': row.get('Sampled Medium'),
                'ValueCount': len(row.index),
                'ResultDateTime': startdate,
                'ResultDateTimeUTCOffset': utcoffset
            })

            self.timeseriesresults[row.get('ResultUUID')] = result

            # Create DataSetsResults object
            _ = self.create(DataSetsResults, commit=False, **{
                'DataSetObj': self.data_set,
                'ResultObj': result
            })

        self.session.commit()

    def parse_datavalues(self):

        result_table = self.tables['DataColumns'].copy()  # type: DataFrame
        result_table = result_table[['ResultUUID', 'Column Label', 'Censor Code', 'Quality Code', 'Time Aggregation Interval', 'Time Aggregation Unit']]

        self.gauge.SetValue(0)

        # for i in xrange(0, self.datavalue_count):
        for i, series in self.tables.get('DataValues').iterrows():
            # values = datavalues[i]

            series.dropna(inplace=True)

            complete = float(i + 1) / float(self.datavalue_count)
            self.update_progress_label('%s/%s' % (i + 1, self.datavalue_count))
            self.gauge.SetValue(complete * 100)

            # series = Series(values, index=colnames)
            # series = Series(row, index=colnames)
            # series.dropna(inplace=True)

            localdt = series.get('LocalDateTime')
            utcoffset = series.get('UTCOffset')

            tsrvs = []
            for _, result in result_table.iterrows():
                label = result.get('Column Label')
                uuid = result.get('ResultUUID')

                tsr = self.timeseriesresults.get(uuid)
                value = series.get(label)

                assert(tsr is not None)

                censor_code = result.get('Censor Code')
                quality_code = result.get('Quality Code')
                timeagg_interval = result.get('Time Aggregation Interval')
                aggregation_unit = self.units.get(result.get('Time Aggregation Unit').lower())

                tsrvs.append(TimeSeriesResultValues(
                    ResultObj=tsr,
                    DataValue=value,
                    ValueDateTime=localdt,
                    ValueDateTimeUTCOffset=utcoffset,
                    CensorCodeCV=censor_code,
                    QualityCodeCV=quality_code,
                    TimeAggregationInterval=timeagg_interval,
                    TimeAggregationIntervalUnitsObj=aggregation_unit
                ))

            self.__commit_tsrvs(tsrvs)

        self._flush()

    def __commit_tsrvs(self, tsrvs):
        self.session.add_all(tsrvs)
        try:
            self.session.commit()
        except (IntegrityError, ProgrammingError):
            self.session.rollback()

            for tsrv in tsrvs:
                self.session.add(tsrv)
                try:
                    self.session.commit()
                except (IntegrityError, ProgrammingError) as e:
                    self.update_output_text('Error (row %s): %s' % (i, e.message))
                    self.session.rollback()