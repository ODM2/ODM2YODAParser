from odm2api.models import Base, TimeSeriesResultValues
from sqlalchemy.exc import IntegrityError, ProgrammingError
import sqlalchemy.ext.declarative.api as api
import pandas as pd
from sqlalchemy import func


class iOutputs:

    def __init__(self):
        pass

    def parseObjects(self, session):
        data = {}

        schema = TimeSeriesResultValues.__table_args__['schema']

        for t in self.get_table_names():

            tmplist = []
            try:
                if t.__tablename__.lower() == "timeseriesresultvalues":
                    # TODO: Test if this works for database connections to mssql and mysql
                    if 'postgresql' in session.bind.name:
                        sql = """SELECT * FROM {}.timeseriesresultvalues""".format(schema)
                    elif 'mssql' in session.bind.name:
                        sql = """SELECT * FROM {}.TimeSeriesResultValues""".format(schema)
                    else:
                        sql = """SELECT * FROM TimeSeriesResultValues"""
                    tbl = pd.read_sql(sql, session.connection().connection.connection)
                    tmplist = tbl
                else:

                    try:
                        for obj in session.query(t).all():
                            # session.expunge(o)
                            tmplist.append(obj)

                    except ProgrammingError as e:

                        print(e.message)

            except IntegrityError as e:
                print(e)
                session.rollback()

            if len(tmplist) > 0:
                data[t.__tablename__] = tmplist

        return data

    def get_table_names(self):
        tables = []
        import inspect
        import sys
        # get a list of all of the classes in the module
        clsmembers = inspect.getmembers(sys.modules["odm2api.models"],
                                        lambda member: inspect.isclass(member) and member.__module__ == "odm2api.models")

        for name, Tbl in clsmembers:
            if isinstance(Tbl, api.DeclarativeMeta):
                # check to see if the schema is already set correctly
                tables.append(Tbl)

        return tables

    def save(self, session, path):
        raise NotImplementedError()

    def accept(self):
        raise NotImplementedError()
