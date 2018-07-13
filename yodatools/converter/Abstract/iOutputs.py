
from odm2api.models import Base


class iOutputs:

    def __init__(self):
        pass

    def parseObjects(self, session):
        data = {}

        for t in self.get_table_names():

            tmplist = []
            try:
                if t.__tablename__.lower() == "timeseriesresultvalues":
                    import pandas as pd
                    sql = """SELECT * FROM TimeSeriesResultValues"""
                    tbl = pd.read_sql(sql, session.connection().connection.connection)
                    tmplist = tbl
                else:
                    for o in session.query(t).all():
                        # session.expunge(o)
                        tmplist.append(o)

            except Exception as e:
                print "error: " + e.message

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
            import sqlalchemy.ext.declarative.api as api
            if isinstance(Tbl, api.DeclarativeMeta):
                # check to see if the schema is already set correctly
                tables.append(Tbl)
        return tables

    def save(self, session, path):
        raise NotImplementedError()

    def accept(self):
        raise NotImplementedError()
