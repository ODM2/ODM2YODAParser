import os
import re

from pubsub import pub

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from odm2api.models import Base
from pandas import isnull, DataFrame


class ExcelParser(object):

    TABLE_NAMES = [
        'Analysis_Results',
        'Organizations',
        'People'
        'ProcessingLevels',
        'Sites',
        'SpatialReferences',
        'SpecimenAnalysisMethods',
        'SpecimenCollectionMethods',
        'Specimens',
        'Units',
        'Varibles'
    ]

    def __init__(self, input_file, session_factory, **kwargs):

        self.input_file = input_file

        self.session = session_factory.getSession()
        self.engine = session_factory.engine

        self.gauge = kwargs.get('gauge', None)

        self.total_rows_to_read = 0
        self.rows_read = 0
        self.workbook = None
        self.sheets = []
        self.name_ranges = {}
        self.tables = {}

        self._init_data(input_file)

    def _init_data(self, *args):
        raise NotImplementedError

    def get_or_create(self, model, values, check_fields=None, filter_by=None, commit=True):  # type: (Base, dict, [str], str|[str], bool) -> Base
        """

        :param model: The model from odm2api.models used to create the object
        :param values: A dict containing the fields to insert into the database (given the record does not exist).
        :param check_fields: A list of strings of required field names (optional).
        :param filter_by: A string or list of strings used to filter queries by. If None, the query will filter using **values (optional).
        :param commit: Boolean value indicating whether or not to commit the transaction.
        :return: An instance of the retrieved or created model.
        :raise ValueError: Raised when a value in values is NaT given the key exists in check_fields
        """
        if check_fields:
            bad_fields = []
            for field in check_fields:
                if isnull(values[field]):
                    bad_fields.append(field)

            if len(bad_fields):
                raise ValueError('Object "{}" is missing required fields: {}'.format(model.__tablename__.title(),
                                                                                     ', '.join(bad_fields)))

        filters = {}

        if isinstance(filter_by, str):
            filters[filter_by] = values.get(filter_by, None)
        elif isinstance(filter_by, list):
            for f in filter_by:
                filters[f] = values.get(f, None)
        else:
            filters = values

        instance = self.get(model, **filters)

        if instance:
            return instance
        else:
            return self.create(model, commit=commit, **values)

    def create(self, model, commit=True, **kwargs):
        instance = model(**kwargs)
        self.session.add(instance)

        if commit:
            self.session.commit()

        return instance

    def get(self, model, **kwargs):
        try:
            return self.session.query(model).filter_by(**kwargs).one()
        except NoResultFound:
            return None


    def _flush(self):
        try:
            self.session.flush()
        except IntegrityError as e:

            if os.getenv('DEBUG') == 'true':
                print(e)

            self.session.rollback()

    def _updateGauge(self, rows_read=1):
        """
        Updates the gauge based on `self.rows_read`
        :return: None
        """
        # Objects are passed by reference in Python :)
        if not getattr(self, 'gauge', None):  # type: wx.Gauge
            return  # No gauge was passed in, but that's ok :)

        self.rows_read += rows_read
        try:
            value = (float(self.rows_read) / float(self.total_rows_to_read)) * 100.0
            self.gauge.SetValue(value)
        except ZeroDivisionError:
            pass

    def get_named_range(self, sheet, coord):
        """
        Returns the range of cells contained in a given worksheet by a given set of coordinates
        :param sheet: string like.
            Name of the worksheet
        :param coord: string like
            String representation of sheet coordinates
        :return: Range of cell(s)
        """
        ws = self.workbook[sheet]
        return ws[coord]

    def get_named_range_value(self, sheet, coord):
        """
        Gets the value of the cell(s) in a given worksheet at a given set of coordinates

        :param sheet: string like.
            Name of the worksheet with the named range
        :param coord: string like.
            String representation of the named range coordinate (e.g. '$A$1')
        :return: Value(s) contained in the named range given by `coord`
        """
        return self.get_named_range(sheet, coord).value

    def get_named_range_cell_value(self, named_range):
        """
        Gets the value of the cell given by named_range. The passed in named range
        should reference only a single cell.


        :param named_range: string like.
            Name of the named range
        :return:
        """
        try:
            nr = self.workbook.defined_names[named_range]
            return self.get_named_range_value(*next(nr.destinations))
        except KeyError:
            return None


    def parse_name(self, fullname):  # type: (str) -> dict
        """
        Parses a full name contained in a string and returns a dict representation
        of the name. Also removes trailing/leading whitespace of the names.

        If `fullname` does not contain a comma, it's assumed `fullname` is formatted as:

            "<first name> <middles name(s)> <last name>"

        If `fullname` contains a comma (e.g. "Doe, John"), then it is assumed `fullname`
        if formatted as:

            "<last name>, <first name> <middle name(s)>"

        :param fullname:
        :return:
        """
        values = re.split(r'\s+', fullname)

        if any([',' in name for name in values]):
            # `fullname` contained a comma (formatted as "<last>, <first> <middle>")
            # so do a little rearranging.
            lastname = values.pop(0).replace(',', '')

            try:
                firstname = values.pop(0)
            except IndexError:
                firstname = ''

            values = [firstname] + values + [lastname]

        names = {
            'first_name': values[0],
            'last_name': values[-1]
        }

        middle = ' '.join(values[1:-1])
        if len(middle):
            names.update(middle_name=middle)


        return names

    def update_progress_label(self, message):
        pub.sendMessage('controller.update_progress_label', message=message)

    def update_output_text(self, message):
        pub.sendMessage('controller.update_output_text', message='%s\n' % message)
