
import datetime
import copy
import yaml

class YamlPrinter():

    _references= {}
    def get_header(self, filetype):
        yoda_header = "---\n"
        yoda_header += "YODA:\n"

        yoda_header += " - {"
        yoda_header += "Version: \"{0}\", ".format("0.1.0")
        yoda_header += "Profile: \"{0}\", ".format(filetype)
        yoda_header += "CreationTool: \"{0}\", ".format("YodaConverter")
        yoda_header += "DateCreated: \"{0}\", ".format(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d'))
        yoda_header += "DateUpdated: \"{0}\"".format(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d'))
        yoda_header += "}\n"
        return yoda_header.replace('None', 'NULL')

    def handle_inheritance(self, apiobjs):
        # break into groups of object types (specimen vs site)
        from collections import defaultdict
        objlist = defaultdict(list)
        text = ""

        for obj in apiobjs:
            objlist[obj.__class__.__name__].append(obj)

        index = 0
        # call current function with each list, send in class name
        for k, v in objlist.items():
            text += self.print_objects(objlist[k],  index)
            index += len(objlist[k])
        return text

    def fill_dict(self, obj):
        for val in ["SpecimenTypeCV", "SiteTypeCV", "CensorCodeCV"]:
            try:
                getattr(obj, val)
            except:
                pass

    def print_objects(self, apiobjs, ind=0):
        # TODO handle inheritance objects
        objname = apiobjs[0].__class__.__name__

        text = objname + ":\n"

        index = 1 + ind
        for obj in apiobjs:

            primarykey = obj.__mapper__.primary_key[0].name
            #add this try/except block to make sure the inherited objects' dictionaries have all the metadata
            self.fill_dict(obj)
            valuedict = obj.__dict__.copy()

            #find the attribute name of the primary key
            for v in valuedict:
                if v.lower() == obj.__mapper__.primary_key[0].name:
                    # pop unwanted items from the dictionary
                    valuedict.pop(v)
                    if v != "BridgeID" and v != "RelationID":
                        primarykey = v
                    else:
                        #what to do if the primary key is BridgeID?
                        if objname[-1] == "s":
                            primarykey = objname[:-1] + "ID"
                        else:
                            primarykey = objname + "ID"
                    break

            #remove all id's from the dictionary
            for k in valuedict.keys():
                if "id" in k.lower() and "uuid" not in k.lower():
                    del valuedict[k]

            #assign the reference value for objects
            for key in dir(obj):
                if "obj" in key.lower():  # key.contains("Obj"):
                    try:
                        att = getattr(obj, key)
                        if att is not None:
                            valuedict[key] = self._references[att]
                        else:
                            valuedict[key] = "NULL"
                    except Exception as e:
                        # print ("cannot find {} in {}. Error:{} in YamlPrinter".format(key, obj.__class__, e))
                        pass

            self._references[obj] = '*{}{:0>4d}'.format(primarykey, index)
            text += ' - &{}{:0>4d} '.format(primarykey, index)
            text += self.print_dictionary(valuedict)
            index += 1
        return text

    def print_dictionary(self, dict):
        from numbers import Number
        final_string = "{"
        for k, v in dict.items():

            #if the item is null don't print it
            if v is None:
                final_string += '{}: NULL, '.format(k)
            elif isinstance(v, Number):
                final_string += '{}: {}, '.format(k, v)
            elif isinstance(v, basestring):
                if '*' in v:
                    final_string += '{}: {}, '.format(k, v)
                else:
                    final_string += '{}: "{}", '.format(k, v)
            elif isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
                final_string += '{}: "{}", '.format(k, v.strftime("%Y-%m-%d %H:%M"))

        final_string = "{}}}\n".format(final_string[:-2])

        return final_string

    def add_to_db(self, objname, file, data):

        if objname in data:
            # check to see if this is an inherited object
            if data[objname][0].__class__ !=data[objname][0]._sa_instance_state.key[0]:
                file.write(self.handle_inheritance(apiobjs=data[objname]))
            else:
                file.write(self.print_objects(data[objname]))

    def generate_ts_objects(self, serial):
        text = 'TimeSeriesResultValues:\n'
        text += '    ColumnDefinitions:\n'
        text += '    - {ColumnNumber: 0001, Label: "ValueDateTime", ODM2Field: ValueDateTime}\n'
        text += '    - {ColumnNumber: 0002, Label: "ValueDateTimeUTCOffset", ODM2Field: ValueDateTimeUTCOffset}\n'
        ind = 3

        meta = serial.groupby('resultid').min()
        del meta["datavalue"]
        del meta["valueid"]

        cross_tab = self.generate_ts_data(serial)

        for index, row in meta.iterrows():
            varname, resultkey, taiuObj = ["", "", ""]
            try:
                unit = self.units_dict[row["timeaggregationintervalunitsid"]]
                # get unit
                taiuObj = self._references[unit]

                result = self.result_dict[index]
                # get result
                resultkey = self._references[result]
                # get varname
                varname = result.VariableObj.VariableCode
                # Change column names from ResultID to VariableCode, then VariableCode & SamplingFeatureCode, then
                # VariableCode & SamplingFeatureCode &
                cross_tab.rename(columns={index: varname}, inplace=True)
                serial.ix[serial.resultid == index] = varname

            except Exception as e:
                print "I am an error" + e.message

            text += '    - {{ColumnNumber: {:0>4d}, Label: "{}", ODM2Field: "DataValue", ' \
                    'Result: {}, CensorCodeCV: "{}", QualityCodeCV: "{}", ' \
                    'TimeAggregationInterval: {}, TimeAggregationIntervalUnitsObj: {}}}' \
                    '\n'.format(ind, varname, resultkey, row["censorcodecv"], row["qualitycodecv"],
                                row["timeaggregationinterval"], taiuObj)
            ind += 1
        text += "    Data:\n"
        text += "    - [[\n"
        text += cross_tab.to_csv(line_terminator='],[\n')
        text += "]]\n"
        return text


    def generate_ts_data(self, serial):

        # Timeseriesresultvalues - ColumnDefinitions:, Data:
        cross_tab = serial.pivot_table(
            index=["valuedatetime", "valuedatetimeutcoffset"],

            columns="resultid",
            values="datavalue")

        # cross_tab = cross_tab.rename(columns={'valuedatetime': 'ValueDateTime', 'valuedatetimeutcoffset': 'ValueDateTimeUTCOffset'})
        cross_tab.index.names = ['ValueDateTime', 'ValueDateTimeUTCOffset']


        return cross_tab

    def parse_meta(self, data):
        self.result_dict = {}
        for res in data["results"]:
            self.result_dict[res.ResultID] = res

        self.units_dict = {}
        for unit in data["units"]:
            self.units_dict[unit.UnitsID] = unit







    def print_yoda(self, out_file, data):
        self.data = data

        if "measurementresultvalues" in data:
            filetype = "SpecimenTimeSeries"
        else:
            filetype = "TimeSeries"

        with open(out_file, 'w') as yaml_schema_file:

            # Header
            yaml_schema_file.write(self.get_header(filetype))
            # Data Set
            self.add_to_db("datasets", yaml_schema_file, data)
            # Organization
            self.add_to_db("organizations", yaml_schema_file, data)
            # People
            self.add_to_db("people", yaml_schema_file, data)
            # Affiliations
            self.add_to_db("affiliations", yaml_schema_file, data)
            # Citations
            self.add_to_db("citations", yaml_schema_file, data)
            # Author Lists
            self.add_to_db("authorlists", yaml_schema_file, data)
            # Data Set Citations
            self.add_to_db("datasetcitations", yaml_schema_file, data)
            # Spatial References
            self.add_to_db("spatialreferences", yaml_schema_file, data)
            # Sampling Features:
            self.add_to_db("samplingfeatures", yaml_schema_file, data)
            # Related Features
            self.add_to_db("relatedfeatures", yaml_schema_file, data)
            # Units
            self.add_to_db("units", yaml_schema_file, data)
            # Annotations
            self.add_to_db("annotations", yaml_schema_file, data)
            # Methods
            self.add_to_db("methods", yaml_schema_file, data)
            # Variables
            self.add_to_db("variables", yaml_schema_file, data)
            # Processing Level
            self.add_to_db("processinglevels", yaml_schema_file, data)
            # Action
            self.add_to_db("actions", yaml_schema_file, data)
            # Feature Action
            self.add_to_db("featureactions", yaml_schema_file, data)
            # Action By
            self.add_to_db("actionby", yaml_schema_file, data)
            # Related Actions
            self.add_to_db("relatedactions", yaml_schema_file, data)
            # Result
            self.add_to_db("results", yaml_schema_file, data)
            self.parse_meta(data)
            # Data Set Results
            self.add_to_db("datasetsresults", yaml_schema_file, data)
            # Measurement Result Values
            self.add_to_db("measurementresultvalues", yaml_schema_file, data)
            # Measurement Result Value Annotations
            self.add_to_db("measurementresultvalueannotations", yaml_schema_file, data)
            # Time Series Result Values
            val = "timeseriesresultvalues"
            if val in data:
                yaml_schema_file.write(self.generate_ts_objects(data[val]))

            yaml_schema_file.write("...")

