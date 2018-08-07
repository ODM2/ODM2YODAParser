import os

from sqlalchemy.exc import IntegrityError


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

    def _flush(self):
        try:
            self._session.flush()
        except IntegrityError as e:

            if os.getenv('DEBUG') == 'true':
                print(e)

            self._session.rollback()
