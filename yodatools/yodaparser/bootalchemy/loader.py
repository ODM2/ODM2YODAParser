from yaml import load
import sys
import logging
from pprint import pformat
from converters import timestamp, timeonly
from sqlalchemy.orm import class_mapper
from sqlalchemy import Unicode, Date, DateTime, Time, Integer, Float, Boolean, String, Binary, inspect
from datetime import datetime
try:
    from sqlalchemy.exc import IntegrityError
except ImportError:
    from sqlalchemy.exceptions import IntegrityError
from functools import partial


# from yodatools.timeseries import convertTimeSeries
import pandas as pd
# removes the warning message for pandas chained_assignment
pd.options.mode.chained_assignment = None


log = logging.Logger('bootalchemy', level=logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log.addHandler(ch)

# Support for SQLAlchemy 0.5 while 0.6 is in beta. This will be removed in future versions.
try:
    from sqlalchemy.dialects.postgresql.base import PGArray
except ImportError:
    log.error('You really should upgrade to SQLAlchemy=>0.6 to get the full bootalchemy experience')
    PGArray = None

class Loader(object):
    """
       Basic Loader

       *Arguments*
          converter
            list of classes in your converter.
          references
            references from an sqlalchemy session to initialize with.
          check_types
            introspect the target converter class to re-cast the data appropriately.
    """

    def try_parsing_date(self, text):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')


    default_encoding = 'utf-8'

    def cast(self, type_, cast_func, value):
        if type(value) == type_:
            return value
        else:
            return cast_func(value)

    def __init__(self, model, references=None, check_types=True):
        self.default_casts = {Integer:int,
                              Unicode: partial(self.cast, unicode, lambda x: unicode(x, self.default_encoding)),
                              Date: timestamp,
                              DateTime: timestamp,
                              Time: timeonly,
                              Float:float,
                              Boolean: partial(self.cast, bool, lambda x: x.lower() not in ('f', 'false', 'no', 'n')),
                              Binary: partial(self.cast, str, lambda x: x.encode('base64'))
        }
        if PGArray:
            self.default_casts[PGArray] = list

        self.source = 'UNKNOWN'
        self.model = model
        if references is None:
            self._references = {}
        else:
            self. _references = references

        if not isinstance(model, list):
            model = [model]

        self.modules = []
        for item in model:
            if isinstance(item, basestring):
                self.modules.append(__import__(item))
            else:
                self.modules.append(item)

        self.check_types = check_types

    def clear(self):
        """
        clear the existing references
        """
        self._references = {}

    def create_obj(self, klass, item):
        """
        create an object with the given data
        """
        # xxx: introspect the class constructor and pull the items out of item that you can, assign the rest
        try:
            obj = klass(**item)
        except TypeError, e:
            self.log_error(e, None, klass, item)
            raise TypeError("The class, %s, cannot be given the items %s. Original Error: %s" %
                            (klass.__name__, str(item), str(e)))
        except AttributeError, e:
            self.log_error(e, None, klass, item)
            raise AttributeError("Object creation failed while initializing a %s with the items %s. Original Error: %s" %
                                 (klass.__name__, str(item), str(e)))
        except KeyError, e:
            self.log_error(e, None, klass, item)
            raise KeyError("On key, %s, failed while initializing a %s with the items %s. %s.keys() = %s" %
                           (str(e), klass.__name__, str(item), klass.__name__, str(klass.__dict__.keys())))

        return obj

    def resolve_value(self, value):
        """
        `value` is a string or list that will be applied to an ObjectName's attribute.
        Link in references when it hits a value that starts with an "*"
        Ignores values that start with an "&"
        Nesting also happens here: Create new objects for values that start with a "!"
        Recurse through lists.
        """
        if isinstance(value, basestring):
            if value.startswith('&'):
                return None
            elif value.startswith('*'):
                if value[1:] in self._references:
                    return self._references[value[1:]]
                else:
                    raise Exception('The pointer %(val)s could not be found. Make sure that %(val)s is declared before it is used.' % { 'val': value })
        elif isinstance(value, dict):
            keys = value.keys()
            if len(keys) == 1 and keys[0].startswith('!'):
                klass_name = keys[0][1:]
                items = value[keys[0]]
                klass = self.get_klass(klass_name)

                if isinstance(items, dict):
                    return self.add_klass_with_values(klass, items)
                elif isinstance(items, list):
                    return self.add_klasses(klass, items)
                else:
                    raise TypeError('You can only give a nested value a list or a dict. You tried to feed a %s into a %s.' %
                                    (items.__class__.__name__, klass_name))

        elif isinstance(value, list):
            return [self.resolve_value(list_item) for list_item in value]

        # an 'assert isinstance(value, basestring) and value[0:1] not in ('&', '*', '!') could probably go here.
        return value

    def has_references(self, item):
        for key, value in item.iteritems():
            if isinstance(value, basestring) and value.startswith('&'):
                return True

    def add_reference(self, key, obj):
        """
        add a reference to the internal reference dictionary
        """
        self._references[key[1:]] = obj

    def set_references(self, obj, item):
        """
        extracts the value from the object and stores them in the reference dictionary.
        """
        for key, value in item.iteritems():
            if isinstance(value, basestring) and value.startswith('&'):
                self._references[value[1:]] = getattr(obj, key)
            if isinstance(value, list):
                for i in value:
                    if isinstance(value, basestring) and i.startswith('&'):
                        self._references[value[1:]] = getattr(obj, value[1:])

    def _check_types(self, klass, obj):
        if not self.check_types:
            return obj
        mapper = class_mapper(klass)
        for table in mapper.tables:
            for key in obj.keys():
                col = table.columns.get(key, None)
                value = obj[key]
                if value is not None and col is not None and col.type is not None:
                    for type_, func in self.default_casts.iteritems():
                        if isinstance(col.type, type_):
                            obj[key] = func(value)
                            break
                if value is None and col is not None and isinstance(col.type, (String, Unicode)):
                    obj[key] = ''
        return obj

    def get_klass(self, klass_name):
        klass = None
        for module in self.modules:
            # try:
            klass = getattr(module, klass_name)
            break
            # except AttributeError:
            # pass
        # check that the class was found.
        if klass is None:
            raise AttributeError('Class %s from %s not found in any module' % (klass_name , self.source))
        return klass

    def obtain_key_value(self, key, value, resolved_values):
        """
        Need to convert 'key' value to be the object equivalent in each table.
        samplefiles:
            PersonID -> PersonObj

        """
        # save off key
        new_obj_key = key

        # pop off old key in order to maintain size of resolved_values when the object version is added to the dictionary
        resolved_values.pop(key)

        # Convert the sqlalchemy object equivalent of <table>Obj from <table>ID
        key = self.get_object_key(new_obj_key)

        # value = self.get_key_value_tuple(new_obj_key, value)

        assert isinstance(key, basestring)
        # assert isinstance(value, tuple)

        return key, value

    def get_object_key(self, key):
        """
        Turn <Table>ID into <Table>Obj

        Since <Table>ID can only store Integers and we are following bootAlchemy's work flow, we need to change <Table>ID into <Table>Obj

        Example:
            If we are working with '<class ODM2.models.AuthorLists>', we are working with the following schema
                {
                    AuthorOrder,
                    BridgeID,
                    CitationID,
                    CitationObj,
                    PersonID,
                    PersonObj
                }

            The algorithm is to change the incoming key value of PersonID into PersonObj and CitationID into CitationObj. The Key
                is paired with the yodaparser object in resolved_values.
        """
        tmp_key = key
        ## Turn Person into People
        tmp_key = tmp_key[:-2] + 'Obj'
        return tmp_key

    def get_key_value_tuple(self, key, value):
        """
        Return a tuple of the <Table> and values needed to build <Table> as listed in the ODM2 Schema

        samplefiles:
            key = PersonID
            key[:-2] == Person. In ODM2, Person no longer exists, so return People
            class = People

            key = SamplingFeatureID
            class = SamplingFeature + 's' = SamplingFeatures (since ODM2 has changed to be plural)
        """

        tmp_key = key


        # Need to handle special cases
        if tmp_key[:-2] == 'Person':
            tmp_key = 'People'

        elif 'Units' in tmp_key[:-2]:
            tmp_key = "Units"

        # Handle plural cases
        else:
            tmp_key = key[:-2] + 's'
        klass = tmp_key

        return (klass, value)

    def add_klass_with_values(self, klass, values):
        """
        klass is a type, values is a dictionary. Returns a new object.
        """
        ref_name = None
        keys = values.keys()

        if len(keys) == 1 and keys[0].startswith('&') and isinstance(values[keys[0]], dict):
            ref_name = keys[0]
            values = values[ref_name] # ie. item.values[0]

        # Values is a dict of attributes and their values for any ObjectName.
        # Copy the given dict, iterate all key-values and process those with special directions (nested creations or links).
        resolved_values = values.copy()

        for key, value in resolved_values.iteritems():

            if value and isinstance(value, basestring) and value.startswith('*'):
                value = self.obtain_object_id(key, value)
                # key, value = self.obtain_key_value(key, value, resolved_values)
            elif "date" in key.lower() and not ("utc" in key.lower()):
                #this has been added to support sqlite,
                #print "%s is a date type. %s"%(key, value)
                if value is not None:
                    try:

                        value = self.try_parsing_date(value)#datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        print ("error converting datetime {}".format(e))
                        # value = datetime.strptime(value, "%Y-%m-%d")


            resolved_values[key] = self.resolve_value(value)


        # _check_types currently does nothingublssdf (unless you call the loaded with a check_types parameter)
        resolved_values = self._check_types(klass, resolved_values)

        obj = self.create_obj(klass, resolved_values)
        self.session.add(obj)

        if ref_name:
            self.add_reference(ref_name, obj)
        if self.has_references(values):
            self.session.flush()
            self.set_references(obj, values)

        return obj


    def obtain_object_id(self, key, value):
        self.session.flush()
        ref = None
        try:
            ref = self._references[value[1:]]

            if key.endswith("ID"):
                # obtain the primary key value
                value = inspect(ref).identity[0]

            elif key.endswith("Obj"):
                # if not 'SamplingFeatureObj' in key:
                #     value = ref
                # else:
                value = inspect(ref.identity[0])


            return value
        except Exception as e:
            return value



    def parse_meta(self, meta):

        col_dict= {}
        for col in meta:
            if "ValueDateTime" not in col["Label"]:
                # print col["Label"]
                value_list = {}

                # dfUnstacked["Label" == col["ODM2Field"]]= col

                for key, value in col.iteritems():
                    if key not in ["ColumnNumber", "ODM2Field"]:
                        value_list[key]= self.resolve_value(value)

                col_dict[col["Label"]] = value_list

        return col_dict


    def loadTimeSeriesResults(self, timeSeries,  session, engine):
        """
        Loads TimeSeriesResultsValues into pandas DataFrame
        """

        try:
            column_labels = timeSeries["Data"][0][0]
            # data_values = timeSeries["Data"][0][1:]
            meta = timeSeries['ColumnDefinitions']
            date_column = meta[0]["Label"]
            utc_column = meta[1]["Label"]
            cross_tab = pd.DataFrame(timeSeries["Data"][0][1:], columns=column_labels)  #, index=date_column)

        except Exception as ex:
            return

        cross_tab.set_index([date_column, utc_column], inplace=True)

        serial = cross_tab.unstack(level=[date_column, utc_column])
        meta_dict = self.parse_meta(meta=meta)

        serial = serial.append(pd.DataFrame(columns=['ResultID', 'CensorCodeCV', 'QualityCodeCV', 'TimeAggregationInterval',
                                            'TimeAggregationIntervalUnitsID']))\
                                            .fillna(0)\
                                            .reset_index()\
                                            .rename(columns={0: 'DataValue'})\
                                            .dropna()

        # print serial.columns

        for k, v in meta_dict.iteritems():
            serial.ix[serial.level_0 == k, 'ResultID'] = v["Result"].ResultID
            serial.ix[serial.level_0 == k, 'CensorCodeCV'] = v["CensorCodeCV"]
            serial.ix[serial.level_0 == k, 'QualityCodeCV'] = v["QualityCodeCV"]
            serial.ix[serial.level_0 == k, 'TimeAggregationInterval'] = v["TimeAggregationInterval"]
            serial.ix[serial.level_0 == k, 'TimeAggregationIntervalUnitsID'] = v["TimeAggregationIntervalUnitsObj"].UnitsID

        del serial['level_0']

        # TODO does this fail for sqlite in memory
        # session.commit()
        # session.close()

        from odm2api.ODM2.models import TimeSeriesResultValues
        tablename = TimeSeriesResultValues.__tablename__
        serial.to_sql(name=tablename,
                         schema=TimeSeriesResultValues.__table_args__['schema'],
                         if_exists='append',
                         chunksize=1000,
                         con=engine,
                         index=False)
        # session.commit()
        # todo add timeseriesresult values objects, should be done with result object
        return serial


    def add_klasses(self, klass, items):
        """
        Returns a list of the new objects. These objects are already in session, so you don't *need* to do anything with them.
        """
        objects = []
        for item in items:
            obj = self.add_klass_with_values(klass, item)
            objects.append(obj)
        return objects

    def from_list(self, session, data):
        """
        Extract data from a list of groups in the form:

        [
            { #start of the first grouping
              ObjectName:[ #start of objects of type ObjectName
                          {'attribute':'value', 'attribute':'value' ... more attributes},
                          {'attribute':'value', 'attribute':'value' ... more attributes},
                          ...
                          }
                          ]
              ObjectName: [ ... more attr dicts here ... ]
              [commit:None] #optionally commit at the end of this grouping
              [flush:None]  #optionally flush at the end of this grouping
            }, #end of first grouping
            { #start of the next grouping
              ...
            } #end of the next grouping
        ]

        Data can also be nested, for example; here are three different ways to do it. The following:

        --- Mountaineering Data Document
        - MountainRegion:
          - name: Appalacians
            coast: East
            climate: { '!Climate': { high: 85, low: 13, precipitation: '54 inches annually' } }
            ranges:
              - '!MountainRange': { name: Blue Ridge Mountains, peak: Mount Mitchell }
              - '!MountainRange': { name: Piedmont Plateau, peak: Piedmont Crescent Peak }
            valleys:
              '!ValleyArea': [ { name: Hudson River Crevasse }, { name: Susquehanna Valleyway } ]

          Is equivalent to this:

        --- Mountaineering Data Document
        - Climate:
            '&appal-climate': { high: 85, low: 13, precipitation: '54 inches annually' }
        - MountainRange: ['&blue-ridge': { name: Blue Ridge Mountains, peak: Mount Mitchell },
                          '&peidmont': { name: Piedmont Plateau, peak: Piedmont Crescent Peak }]
        - ValleyArea:
          - '&hudson':
              name: Hudson River Crevasse
          - '&susq':
              name: Susquehanna Valleyway
        - MountainRegion:
          - name: Appalacians
            coast: East
            climate: '*appal-climate'
            ranges: ['*blue-ridge', '*peidmont']
            valleys: ['*hudson', '*susq']

        However, the nested data is not and cannot be added to the list of references. It is anonymous in that sense.

        Careful! Here are some pitfalls:

        This would double list the valleys. Not good. Like saying "valleys: [['*hudson', '*susq']]"
            valleys:
              - '!ValleyArea': [ { name: Hudson River Crevasse }, { name: Susquehanna Valleyway } ]

        This is not valid:
            climate: '!Climate': { high: 85, low: 13, precipitation: '54 inches annually' }

        Also, literal tags, like !Climate (without quotes), do not work, and will generally break.
        """
        self.session = session
        klass = None
        item = None
        group = None
        skip_keys = ['flush', 'commit', 'clear']
        try:
            for group in data:
                for name, items in group.iteritems():
                    if name not in skip_keys:
                        klass = self.get_klass(name)
                        self.add_klasses(klass, items)

                # session.flush()

                if 'flush' in group:
                    session.flush()
                if 'commit' in group:
                    session.commit()
                if 'clear' in group:
                    self.clear()

        except AttributeError, e:
            if hasattr(item, 'iteritems'):
                missing_refs = [(key, value) for key, value in item.iteritems() if isinstance(value,basestring) and value.startswith('*')]
                self.log_error(e, data, klass, item)
                if missing_refs:
                    log.error('*'*80)
                    log.error('It is very possible you are missing a reference, or require a "flush:" between blocks to store the references')
                    log.error('here is a list of references that were not accessible (key, value): %s'%missing_refs)
                    log.error('*'*80)
            else:
                self.log_error(e, data, klass, item)
        ## except IntegrityError, e:
        ##     raise IntegrityError("Error while inserting %s. Original Error: %s" %
        ##         (klass.__name__, str(item), str(e)))
        #except Exception, e:
        #    self.log_error(sys.exc_info()[2], data, klass, item)
        #    raise

        self.session = None
        return self._references

    def log_error(self, e, data, klass, item):
        log.error('error occured while loading yaml data with output:\n%s'%pformat(data))
        log.error('references:\n%s'%pformat(self._references))
        log.error('class: %s'%klass)
        log.error('item: %s'%item)
        import traceback
        log.error(traceback.format_exc(e))

    def merge_dicts(self, *dict_args):
        '''
        Given any number of dicts, shallow copy and merge into a new dict,
        precedence goes to key value pairs in latter dicts.
        '''
        result = []

        if isinstance(dict_args, tuple):
            for i in dict_args:
                for dictionary in i:
                    result.append(dictionary)
        return result

class YamlLoader(Loader):

    def loadf(self, session, filename):
        """
        Load a yaml file by filename.
        """
        self.source = filename
        s = open(filename).read()
        return self.loads(session, s)

    def loads(self, session, s):
        """
        Load a yaml string into the database.
        """
        data = load(s)
        if data:
            return self.from_list(session, data)
