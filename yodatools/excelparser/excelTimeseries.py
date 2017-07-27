import os
import openpyxl
from odm2api.ODM2.models import *
import time
import string

import xlrd
import pandas as pd


class ExcelTimeseries():

    # https://automatetheboringstuff.com/chapter12/
    def __init__(self, input_file, **kwargs):

        self.input_file = input_file
        self.gauge = None
        self.total_rows_to_read = 0
        self.rows_read = 0

        if 'gauge' in kwargs:
            self.gauge = kwargs['gauge']

        self.workbook = None
        self.sheets = []
        self.name_ranges = None
        self.tables = {}
        self._init_data(input_file)

    def parse(self, session_factory):

        self._session = session_factory.getSession()
        self._engine = session_factory.engine

        self.tables = self.get_table_name_ranges()


        self.parse_affiliations()
        self.parse_datasets()
        self.parse_methods()
        self.parse_variables()
        self.parse_units()
        self.parse_processing_level()
        self.parse_sampling_feature()

        # self.parse_specimens()
        # self.parse_analysis_results()
        self.parse_data_values()

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
        if named_range is not None:
            return named_range.attr_text.split('!')[1].replace('$', '')
        return None

    def get_range_value(self, range_name, sheet):
        value = None
        named_range = self.workbook.get_named_range(range_name)
        range = self.get_range_address(named_range)
        if range:
            value = sheet[range].value
        return value

    def _init_data(self, file_path):
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


    def __updateGauge(self):
        # Objects are passed by reference in Python :)
        if not self.gauge:
            return  # No gauge was passed in, but that's ok :)

        self.rows_read += 1
        value = float(self.rows_read) / self.total_rows_to_read * 100.0
        self.gauge.SetValue(value)

    def get_sheet_and_table(self, sheet_name):
        if sheet_name not in self.tables:
            return [], []
        sheet = self.workbook.get_sheet_by_name(sheet_name)
        tables = self.tables[sheet_name]

        return sheet, tables


    def parse_datasets(self):

        CONST_DATASET = 'Dataset Citation'

        sheet, tables = self.get_sheet_and_table(CONST_DATASET)

        dataset= DataSets()
        dataset.DataSetUUID = self.get_range_value("DatasetUUID", sheet)
        dataset.DataSetTypeCV = self.get_range_value("DatasetType", sheet)
        dataset.DataSetCode = self.get_range_value("DatasetCode", sheet)
        dataset.DataSetTitle = self.get_range_value("DatasetTitle", sheet)
        dataset.DataSetAbstract = self.get_range_value("DatasetType", sheet)
        self._session.add(dataset)
        self._session.flush()
        self.dataset = dataset


        citation = Citations()
        citation.RelationshipTypeCV = self.get_range_value("DatasetCitationRelationship", sheet)
        citation.Title = self.get_range_value("CitationTitle", sheet)
        citation.Publisher = self.get_range_value("Publisher", sheet)
        citation.PublicationYear = self.get_range_value("PublicationYear", sheet)
        citation.CitationLink = self.get_range_value("CitationLink", sheet)
        # citation.DOI
        self._session.add(citation)
        self._session.flush()


        #TODO only do this if the citation is set
        authors = []
        author_order = 1
        #this is for AuthorListInfo
        for table in tables:
            cells = sheet[self.get_range_address(table)]
            if table.name == "AuthorList_Table":
                for row in cells:
                    # Action By
                    names = filter(None, row[1].value.split(' '))
                    if len(names) > 2:
                        last_name = names[2].strip()
                    else:
                        last_name = names[1].strip()
                    first_name = names[0].strip()
                    person = self._session.query(People).filter_by(PersonLastName=last_name,
                                                                   PersonFirstName=first_name).first()

                    author = AuthorLists()
                    author.AuthorOrder = author_order
                    author.PersonObj = person
                    author.CitationObj = citation
                    authors.append(author)
                    author_order += 1

                    self.__updateGauge()

        self._session.add_all(authors)
        self._session.flush()

    def parse_units(self):
        CONST_UNITS = 'Units'

        sheet, tables = self.get_sheet_and_table(CONST_UNITS)

        if not len(tables):
            print "No Units found"
            return

        units = []
        for table in tables:
            cells = sheet[self.get_range_address(table)]

            for row in cells:
                unit = Units()
                unit.UnitsTypeCV = row[0].value
                unit.UnitsAbbreviation = row[1].value
                unit.UnitsName = row[2].value
                unit.UnitsLink = row[3].value

                if unit.UnitsTypeCV is not None:
                    units.append(unit)

                self.__updateGauge()

        self._session.add_all(units)
        self._session.flush()

    def parse_affiliations(self):  # rename to Affiliations
        SHEET_NAME = 'People and Organizations'
        sheet, tables = self.get_sheet_and_table(SHEET_NAME)

        if not len(tables):
            print "No affiliations found"
            return []

        def parse_organizations(org_table, session):
            organizations = {}

            cells = sheet[self.get_range_address(org_table)]
            for row in cells:
                org = Organizations()
                org.OrganizationTypeCV = row[0].value
                org.OrganizationCode = row[1].value
                org.OrganizationName = row[2].value
                org.OrganizationDescription = row[3].value
                org.OrganizationLink = row[4].value
                session.add(org)
                organizations[org.OrganizationName] = org
                self.__updateGauge()

            return organizations

        def parse_authors(author_table):
            authors = []
            cells = sheet[self.get_range_address(author_table)]
            for row in cells:
                ppl = People()
                org = Organizations()
                aff = Affiliations()

                ppl.PersonFirstName = row[0].value.strip()
                ppl.PersonMiddleName = row[1].value
                ppl.PersonLastName = row[2].value.strip()

                org.OrganizationName = row[3].value
                aff.AffiliationStartDate = row[4].value
                # aff.AffiliationEndDate = row[6].value
                # aff.PrimaryPhone = row[7].value
                aff.PrimaryEmail = row[5].value
                aff.PrimaryAddress = row[6].value
                # aff.PersonLink = row[10].value

                aff.OrganizationObj = org
                aff.PersonObj = ppl

                authors.append(aff)


            return authors

        # Combine table and authors

        orgs = {}
        affiliations = []
        for table in tables:
            if 'People_Table' == table.name:
                affiliations = parse_authors(table)
            else:
                orgs = parse_organizations(table, self._session)

        self._session.flush()

        for aff in affiliations:
            if aff.OrganizationObj.OrganizationName in orgs:
                aff.OrganizationObj = orgs[aff.OrganizationObj.OrganizationName]

        self._session.add_all(affiliations)
        self._session.flush()

    def parse_processing_level(self):
        CONST_PROC_LEVEL = 'Processing Levels'
        sheet, tables = self.get_sheet_and_table(CONST_PROC_LEVEL)

        if not len(tables):
            print "No processing levels found"
            return []

        processing_levels = []
        for table in tables:
            cells = sheet[self.get_range_address(table)]

            for row in cells:
                if row[0].value is not None:
                    proc_lvl = ProcessingLevels()
                    proc_lvl.ProcessingLevelCode = row[0].value
                    proc_lvl.Definition = row[1].value
                    proc_lvl.Explanation = row[2].value
                    processing_levels.append(proc_lvl)

                self.__updateGauge()

        # return processing_levels
        self._session.add_all(processing_levels)
        self._session.flush()

    def parse_sampling_feature(self):
        SHEET_NAME = 'Sampling Features'
        sheet, tables = self.get_sheet_and_table(SHEET_NAME)

        sites = []
        for table in tables:
            cells = sheet[self.get_range_address(table)]

        spatial_references = self.parse_spatial_reference()
        elevation_datum = self.get_range_value("ElevationDatum", sheet)
        spatial_ref_name = self.get_range_value("LatLonDatum", sheet)
        spatial_references_obj = spatial_references[spatial_ref_name]

        for row in cells:
            if all([row[1].value, row[2].value, row[3].value]):# are all of the required elements present
                site = Sites()
                site.SamplingFeatureUUID = row[0].value
                site.SamplingFeatureTypeCV = row[1].value
                site.SamplingFeatureGeotypeCV= row[2].value
                site.SamplingFeatureCode = row[3].value
                site.SamplingFeatureName = row[4].value
                site.SamplingFeatureDescription = row[5].value
                site.FeatureGeometryWKT = row[6].value

                site.Elevation_m = row[7].value

                site.SiteTypeCV = row[10].value
                site.Latitude = row[11].value
                site.Longitude = row[12].value
                site.ElevationDatumCV = elevation_datum
                site.SpatialReferenceObj = spatial_references_obj

                sites.append(site)
                self.__updateGauge()

            self._session.add_all(sites)
            self._session.flush()



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

    def parse_methods(self):
        CONST_METHODS = "Methods"
        sheet, tables = self.get_sheet_and_table(CONST_METHODS)

        if not len(tables):
            print "No methods found"
            return []

        for table in tables:
            cells = sheet[self.get_range_address(table)]

            for row in cells:
                method = Methods()
                method.MethodTypeCV = row[0].value
                method.MethodCode = row[1].value
                method.MethodName = row[2].value
                method.MethodDescription = row[3].value
                method.MethodLink = row[4].value

                # If organization does not exist then it returns None
                org = self._session.query(Organizations).filter_by(OrganizationName=row[5].value).first()
                method.OrganizationObj = org

                if method.MethodCode:  # Cannot store empty/None objects
                    self._session.add(method)

                self.__updateGauge()

        self._session.flush()

    def parse_variables(self):

        CONST_VARIABLES = "Variables"

        if CONST_VARIABLES not in self.tables:
            print "No Variables found"
            return []

        sheet = self.workbook.get_sheet_by_name(CONST_VARIABLES)
        tables = self.tables[CONST_VARIABLES]

        for table in tables:
            cells = sheet[self.get_range_address(table)]
            for row in cells:
                var = Variables()
                var.VariableTypeCV = row[0].value
                var.VariableCode = row[1].value
                var.VariableNameCV = row[2].value
                var.VariableDefinition = row[3].value
                var.SpeciationCV = row[4].value

                if row[5].value is not None:
                    if row[5].value == 'NULL':
                        #TODO break somehow because not all required data is filled out
                        print "All Variables must contain a valid No Data Value!"
                        var.NoDataValue = None
                    else:
                        var.NoDataValue = row[5].value

                if var.NoDataValue is not None:  # NoDataValue cannot be None
                    self._session.add(var)

                self.__updateGauge()

        self._session.flush()

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
                    method = self._session.query(Methods).filter_by(MethodCode=row[4].value).first()
                    action.MethodObj = method
                    #TODO ActionType
                    action.ActionTypeCV = "Observation"
                    action.BeginDateTime = start_date
                    action.BeginDateTimeUTCOffset = utc_offset
                    action.EndDateTime = end_date
                    action.EndDateTimeUTCOffset = utc_offset

                    # Feature Actions
                    sampling_feature = self._session.query(SamplingFeatures)\
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

                    person = self._session.query(People).filter_by(PersonLastName=last_name, PersonFirstName=first_name).first()
                    affiliations = self._session.query(Affiliations).filter_by(PersonID=person.PersonID).first()
                    act_by.AffiliationObj = affiliations
                    act_by.ActionObj = action
                    act_by.IsActionLead = True


                    # self._session.no_autoflush
                    self._session.flush()

                    self._session.add(action)
                    self._session.flush()
                    self._session.add(feat_act)
                    self._session.add(act_by)
                    # self._session.add(related_action)
                    self._session.flush()
                    # Measurement Result (Different from Measurement Result Value) also creates a Result
                    variable = self._session.query(Variables).filter_by(VariableCode=row[7].value).first()


                    units_for_result = self._session.query(Units).filter_by(UnitsName=row[8].value).first()
                    proc_level = self._session.query(ProcessingLevels).filter_by(ProcessingLevelCode=row[9].value).first()

                    units_for_agg = self._session.query(Units).filter_by(UnitsName=row[12].value).first()

                    # series_result.IntendedTimeSpacing = row[11].value
                    # series_result.IntendedTimeSpacingUnitsObj = units_for_agg
                    series_result.AggregationStatisticCV = row[13].value
                    series_result.ResultUUID = row[2].value
                    series_result.FeatureActionObj = feat_act
                    series_result.ResultTypeCV = row[6].value
                    series_result.VariableObj = variable
                    series_result.UnitsObj = units_for_result
                    series_result.ProcessingLevelObj = proc_level
                    #TODO
                    series_result.StatusCV = "Unknown"
                    series_result.SampledMediumCV = row[11].value
                    series_result.ValueCount = value_count
                    #TODO
                    series_result.ResultDateTime = start_date

                    self._session.add(series_result)
                    self._session.flush()

                    if self.dataset is not None:
                        #DataSetsResults
                        dataset_result.DataSetObj = self.dataset
                        dataset_result.ResultObj = series_result
                        self._session.add(dataset_result)

                    # Timeseries Result Value Metadata

                    my_meta = {}
                    my_meta["Result"] = series_result
                    my_meta["CensorCodeCV"] = row[14].value
                    my_meta["QualityCodeCV"] = row[15].value
                    #TODO
                    my_meta["TimeAggregationInterval"] = row[11].value
                    my_meta["TimeAggregationIntervalUnitsObj"] = units_for_agg

                    metadata[row[1].value] = my_meta

                    # self._session.add(measure_result_value)
                    self._session.flush()

                    self.__updateGauge()

        print "convert from cross tab to serial"
        self.load_time_series_values(data_values, metadata)

    def load_time_series_values(self, cross_tab, meta_dict):
        """
        Loads TimeSeriesResultsValues into pandas DataFrame
        """

        date_column = "LocalDateTime"
        utc_column = "UTCOffset"

        cross_tab.set_index([date_column, utc_column], inplace=True)

        serial = cross_tab.unstack(level=[date_column, utc_column])

        #add all the columns we need and clean up the dataframe
        serial = serial.append(
            pd.DataFrame(columns=['ResultID', 'CensorCodeCV', 'QualityCodeCV', 'TimeAggregationInterval',
                                  'TimeAggregationIntervalUnitsID'])) \
            .fillna(0) \
            .reset_index() \
            .rename(columns={0: 'DataValue'}) \
            .rename(columns={'LocalDateTime': 'ValueDateTime', 'UTCOffset': 'ValueDateTimeUTCOffset'}) \
            .dropna()

        for k, v in meta_dict.iteritems():
            serial.ix[serial.level_0 == k, 'ResultID'] = v["Result"].ResultID
            serial.ix[serial.level_0 == k, 'CensorCodeCV'] = v["CensorCodeCV"]
            serial.ix[serial.level_0 == k, 'QualityCodeCV'] = v["QualityCodeCV"]
            serial.ix[serial.level_0 == k, 'TimeAggregationInterval'] = v["TimeAggregationInterval"]
            serial.ix[serial.level_0 == k, 'TimeAggregationIntervalUnitsID'] = v["TimeAggregationIntervalUnitsObj"].UnitsID

        del serial['level_0']

        # TODO does this fail for sqlite in memory
        # self._session.close()
        tablename = TimeSeriesResultValues.__tablename__
        serial.to_sql(name=tablename,
                      schema=TimeSeriesResultValues.__table_args__['schema'],
                      if_exists='append',
                      chunksize=1000,
                      con=self._engine,
                      index=False)
        self._session.flush()
        return serial