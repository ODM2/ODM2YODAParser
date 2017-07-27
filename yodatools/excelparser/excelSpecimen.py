import os
import openpyxl
from odm2api.ODM2.models import *
from yodatools.converter.Abstract import iInputs
import pandas
import time
import string


class ExcelSpecimen():
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
                sheet, dimensions = name_range.attr_text.split('!')
                sheet = sheet.replace('\'', '')

                if sheet in table_name_range:
                    table_name_range[sheet].append(name_range)
                else:
                    table_name_range[sheet] = [name_range]

                self.count_number_of_rows_to_parse(dimensions=dimensions)

        return table_name_range

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


    def parse(self, session_factory):
        """
        If any of the methods return early, then check that they have the table ranges
        The table range should exist in the tables from get_table_name_range()
        :param :
        :return:
        """

        self._session = session_factory.getSession()
        self._engine = session_factory.engine

        self.tables = self.get_table_name_ranges()

        start = time.time()


        self.parse_affiliations()
        self.parse_datasets()
        self.parse_methods()
        self.parse_variables()
        self.parse_units()
        self.parse_processing_level()
        self.parse_sampling_feature()
        self.parse_sites()
        self.parse_specimens()
        self.parse_analysis_results()

        # self._session.commit()

        end = time.time()
        print(end - start)

        return True

    def __updateGauge(self):
        # Objects are passed by reference in Python :)
        if not self.gauge:
            return  # No gauge was passed in, but that's ok :)

        self.rows_read += 1
        value = float(self.rows_read) / self.total_rows_to_read * 100.0
        self.gauge.SetValue(value)

    def parse_datasets(self):

        CONST_DATASET = 'Dataset Citation'

        sheet, tables = self.get_sheet_and_table(CONST_DATASET)

        dataset = DataSets()
        dataset.DataSetUUID = self.get_range_value("DatasetUUID", sheet)
        dataset.DataSetTypeCV = self.get_range_value("DatasetType", sheet)
        dataset.DataSetCode = self.get_range_value("DatasetCode", sheet)
        dataset.DataSetTitle = self.get_range_value("DatasetTitle", sheet)
        dataset.DataSetAbstract = self.get_range_value("DatasetType", sheet)
        self._session.add(dataset)
        self._session.flush()
        self.data_set = dataset


    def parse_analysis_results(self):
        SHEET_NAME = "Analysis_Results"
        sheet, tables = self.get_sheet_and_table(SHEET_NAME)

        if not len(tables):
            print "No analysis result found"
            return

        for table in tables:
            cells = sheet[self.get_range_address(table)]
            for row in cells:

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
                sampling_feature = self._session.query(SamplingFeatures)\
                    .filter_by(SamplingFeatureCode=row[1].value)\
                    .first()

                feat_act.SamplingFeatureObj = sampling_feature
                feat_act.ActionObj = action

                # Action By
                first_name, last_name = row[8].value.split(' ')
                person = self._session.query(People).filter_by(PersonLastName=last_name).first()
                affiliations = self._session.query(Affiliations).filter_by(PersonID=person.PersonID).first()
                act_by.AffiliationObj = affiliations
                act_by.ActionObj = action
                act_by.IsActionLead = True

                related_action.ActionObj = action
                related_action.RelationshipTypeCV = "Is child of"
                collectionAction = self._session.query(FeatureActions)\
                    .filter(FeatureActions.FeatureActionID == SamplingFeatures.SamplingFeatureID)\
                    .filter(SamplingFeatures.SamplingFeatureCode == row[1].value)\
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
                self._session.flush()

                self.__updateGauge()

    def parse_sites(self):
        return self.parse_sampling_feature()

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
            #TODO check if required people have value
            for row in cells:
                ppl = People()
                org = Organizations()
                aff = Affiliations()

                ppl.PersonFirstName = row[0].value.strip() if row[0].value else ""
                ppl.PersonMiddleName = row[1].value.strip() if row[1].value else ""
                ppl.PersonLastName = row[2].value.strip() if row[2].value else ""

                org.OrganizationName = row[3].value
                aff.AffiliationStartDate = row[5].value
                aff.AffiliationEndDate = row[6].value
                aff.PrimaryPhone = row[7].value
                aff.PrimaryEmail = row[8].value
                aff.PrimaryAddress = row[9].value
                aff.PersonLink = row[10].value

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

    def get_sheet_and_table(self, sheet_name):
        # if sheet_name not in self.tables:
        #     return [], []
        # sheet = self.workbook.get_sheet_by_name(sheet_name)
        # tables = self.tables[sheet_name]
        sheet = self.workbook.get_sheet_by_name(sheet_name)
        if sheet_name not in self.tables:
            tables = []
        else:
            tables = self.tables[sheet_name]

        return sheet, tables

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

        if SHEET_NAME not in self.tables:
            if 'Sites' in self.tables:
                SHEET_NAME = 'Sites'
            else:
                print "No sampling features/sites found"
                return []

        sheet = self.workbook.get_sheet_by_name(SHEET_NAME)
        tables = self.tables[SHEET_NAME]

        sites_table = tables[0] if tables[0].name == 'Sites_Table' else None
        elevation_datum_range = self.workbook.get_named_range("ElevationDatum")
        spatial_ref_name_range = self.workbook.get_named_range("LatLonDatum")

        # spatial_ref_table = tables[0] if tables[0].name == 'SitesDatumCV_Table' else tables[1]

        # def parse_sites_datum_cv(sheet, spatial_reference_table):
        #     result = {}
        #     cells = sheet[spatial_reference_table.attr_text.split('!')[1].replace('$', '')]
        #     result['elevation_datum_cv'] = cells[0][1].value
        #     result['latlon_datum_cv'] = cells[1][1].value
        #     return result

        # sites_datum = parse_sites_datum_cv(sheet, spatial_ref_table)
        spatial_references = self.parse_spatial_reference()

        sites = []
        cells = sheet[self.get_range_address(sites_table)]


        elevation_datum = sheet[self.get_range_address(elevation_datum_range)].value
        # elevation_datum = sites_datum['elevation_datum_cv']
        spatial_ref_name = sheet[self.get_range_address(spatial_ref_name_range)].value.encode('utf-8')
        # spatial_ref_name = sites_datum['latlon_datum_cv']
        spatial_references_obj = spatial_references[spatial_ref_name]

        for row in cells:
            site = Sites()
            site.SamplingFeatureUUID = row[0].value
            site.SamplingFeatureCode = row[1].value
            site.SamplingFeatureName = row[2].value
            site.SamplingFeatureDescription = row[3].value
            site.FeatureGeometryWKT = row[4].value
            site.Elevation_m = row[5].value
            site.SamplingFeatureTypeCV = "Site"
            site.SiteTypeCV = row[6].value
            site.Latitude = row[7].value
            site.Longitude = row[8].value
            site.ElevationDatumCV = elevation_datum
            site.SpatialReferenceObj = spatial_references_obj

            sites.append(site)

            self._session.add(site)
            self._session.flush()

            self.__updateGauge()

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

        for table in tables:
            cells = sheet[self.get_range_address(table)]

            for row in cells:
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
                sampling_feature = self._session.query(SamplingFeatures).filter_by(SamplingFeatureCode=row[7].value).first()
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

                self.__updateGauge()

        self._session.flush()  # Need to set the RelatedFeature.RelatedFeatureID before flush will work

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
                        #TODO break somehow because not all required data is not filled out
                        print "All Variables must contain a valid No Data Value!"
                        var.NoDataValue = None
                    else:
                        var.NoDataValue = row[5].value

                if var.NoDataValue is not None:  # NoDataValue cannot be None
                    self._session.add(var)

                self.__updateGauge()

        self._session.flush()



