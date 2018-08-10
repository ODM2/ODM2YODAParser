import os

from sqlalchemy.exc import IntegrityError

from odm2api.models import Base
from pandas import isnull


class ExcelParser(object):

    TABLE_NAMES = [
        'DataColumns',
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

    def __init__(self):
        pass

    def get_or_create(self, model, values, check_fields=None, filter_by=None):  # type: (Base, dict, [str], str|[str]) -> Base
        """

        :param model: The model from odm2api.models used to create the object
        :param values: A dict containing the fields to insert into the database if the object does not already exist.
        :param check_fields: A list of strings of required field names (optional).
        :param filter_by: A string or list of strings used to filter queries by. If None, the query will filter using **values (optional).
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

        instance = self.session.query(model).filter_by(**filters).first()

        if instance:
            return instance
        else:
            return self.create(model, values)

    def create(self, model, values, commit=True):
        instance = model(**values)
        self.session.add(instance)

        if commit:
            self.session.commit()

        return instance

    def _flush(self):
        try:
            self._session.flush()
        except IntegrityError as e:

            if os.getenv('DEBUG') == 'true':
                print(e)

            self._session.rollback()

    def _updateGauge(self):
        """
        Updates the gauge based on `self.rows_read`
        :return: None
        """
        # Objects are passed by reference in Python :)
        if not getattr(self, 'gauge', None):  # type: wx.Gauge
            return  # No gauge was passed in, but that's ok :)

        self.rows_read += 1
        try:
            value = (float(self.rows_read) / self.total_rows_to_read * 100.0) / 2.

            if value >= self.gauge.GetValue():
                self.gauge.SetValue(value)

        except ZeroDivisionError:
            pass

    def get_named_range(self, sheet, coord):
        ws = self.workbook[sheet]
        return ws[coord]

    def get_named_range_value(self, sheet, coord):
        return self.get_named_range(sheet, coord).value