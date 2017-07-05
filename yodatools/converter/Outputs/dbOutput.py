from yodatools.converter.Abstract import iOutputs
from odm2api.ODMconnection import dbconnection
from odm2api.ODM2.services import *
from odm2api.ODM2.models import setSchema, _changeSchema, Sites, Results, SamplingFeatures, Specimens, MeasurementResults, TimeSeriesResults
import logging
import sqlite3

class dbOutput(iOutputs):

    def __init__(self):
        self.added_objs = {}

    def connect_to_db(self, connection_string):
        self.session_factory_out = dbconnection.createConnectionFromString(connection_string)
        self._session_out = self.session_factory_out.getSession()
        self._engine_out = self.session_factory_out.engine
        setSchema(self._engine_out)
        return self._session_out

    def save(self, session, connection_string):
        self.session_in = session
        self.data = self.parseObjects(session)
        self.connect_to_db(connection_string)

        #units
        self.check("units", self.data)

        #add the rest of the metadata.
        self.check_results(self.data)

        # measurementResultValues
        self.check("measurementresultvalues", self.data)
        # annotations
        self.check("annotation", self.data)
        # MeasurementResultValueAnnotations
        self.check("measurementresultvalueannotations", self.data)

        # timeseriesresultvalues - ColumnDefinitions:, data:
        # self._session_out.commit()
        val = "timeseriesresultvalues"
        if val in self.data:
            self.save_ts(self.data[val])

        self._session_out.commit()


    def check_results(self, data):

        for obj in data["results"]:

            uuid = {}
            uuid["ResultUUID"] = str(obj.ResultUUID)
            instance = self._session_out.query(Results).filter_by(**uuid).first()
            if instance:
                # result
                self.added_objs[obj] = instance.ResultID

            else:
                # select all from actionby where action id is the same
                import odm2api.ODM2.models as model

                FeatureAction = obj.FeatureActionObj
                Action = FeatureAction.ActionObj
                Method = Action.MethodObj
                mOrganization = Method.OrganizationObj
                SamplingFeatures = FeatureAction.SamplingFeatureObj
                Variables = obj.VariableObj
                ProcLevel = obj.ProcessingLevelObj


                datasetResults = self.session_in.query(model.DataSetsResults).filter_by(ResultID = obj.ResultID).all()
                Dataset = datasetResults[0].DataSetObj
                # datasets
                self.add_to_db(Dataset)
                # organization
                self.add_to_db(mOrganization)
                dsCitations = self.session_in.query(model.DataSetCitations).filter_by(DataSetID = Dataset.DatasetID).all()


                for dscit in dsCitations:

                    Citation = dscit.CitationObj
                    # citations
                    self.add_to_db(Citation)
                    # authorlists
                    authorlist= self.session_in.query(model.AuthorLists).filter_by(CitationID=Citation.CitationID).all()
                    for Author in authorlist:
                        Person = Author.PersonObj
                        self.add_to_db(Person)
                        self.add_to_db(Author)

                # datasetcitations
                self.add_to_db(dsCitations)
                # spatialreferences
                self.add_to_db(SamplingFeatures.SpatialReferenceObj)
                # samplingfeatures
                self.add_to_db(SamplingFeatures)

                # relatedfeatures
                relatedfeatures = self.session_in.query(model.RelatedFeatures)\
                    .filter_by(SamplingFeatureID=SamplingFeatures.SamplingFeatureID)\
                    .filter_by(RelatedFeatureID=SamplingFeatures.SamplingFeatureID).all()
                for rf in relatedfeatures:
                    self.add_to_db(rf.SpatialReferenceObj)
                    self.add_to_db(rf.RelatedFeatureObj)
                    self.add_to_db(rf.SamplingFeatureObj)
                    self.add_to_db(rf)

                # methods
                self.add_to_db(Method)
                # variables
                self.add_to_db(Variables)
                # proc level
                self.add_to_db(ProcLevel)
                # action
                self.add_to_db(Action)
                # featureaction
                self.add_to_db(FeatureAction)
                # result
                self.add_to_db(obj)

                ActionBys = self.session_in.query(model.ActionBy).filter_by(ActionID=Action.ActionID).all()
                for actionby in ActionBys:
                    Affiliation = actionby.AffiliationObj
                    People = Affiliation.PersonObj
                    aOrganization = Affiliation.OrganizationObj
                    self.add_to_db(People)
                    self.add_to_db(aOrganization)
                    self.add_to_db(Affiliation)
                    # actionby
                    self.add_to_db(actionby)

                # relatedactions
                relatedActions = self.session_in.query(model.RelatedActions) \
                    .filter_by(ActionID=Action.ActionID) \
                    .filter_by(RelatedActionID=Action.RelatedActionID).all()
                for ra in relatedActions:
                    self.add_to_db(ra.RelatedActionObj)
                    self.add_to_db(ra.ActionObj)
                    self.add_to_db(ra)


                # datasetresults
                for dr in datasetResults:
                    self.add_to_db(dr)

        self._session_out.commit()

    def save_ts(self, values):
        """

        :param values: pandas dataframe
        :return:
        """

        #TODO change ResultID
        from odm2api.ODM2.models import TimeSeriesResultValues
        tablename = TimeSeriesResultValues.__tablename__
        values.to_sql(name=tablename,
                      schema=TimeSeriesResultValues.__table_args__['schema'],
                      if_exists='append',
                      chunksize=1000,
                      con=self._engine_out,
                      index=False)


    def check(self, objname, data):
        if objname in data:
            for value in data[objname]:
                self.add_to_db(value)



    def add_to_db(self,  obj):
        try:
            _changeSchema(None)
            self.fill_dict(obj)
            valuedict = obj.__dict__.copy()
            valuedict = self.get_new_objects(obj, valuedict)
            valuedict.pop("_sa_instance_state")

            #delete primary key
            for v in valuedict.keys():
                if v.lower() == obj.__mapper__.primary_key[0].name:
                    del valuedict[v]
                elif "obj" in v.lower():
                    del valuedict[v]

            model = type(obj)
            #add new object to the session
            new_obj = self.get_or_create(self._session_out, model, **valuedict)

            ## save the new Primary key to the dictionary

            # find the primary key
            for k in new_obj.__dict__.keys():
                if k.lower() == new_obj.__mapper__.primary_key[0].name:
                    new_pk = new_obj.__dict__[k]
                    # pk = k
                    break
            # new_pk = getattr(new_obj, pk)

            # save pk to dictionary
            self.added_objs[obj] = new_pk


        except Exception as e:
            print e
            self._session_out.rollback()
            # raise e



    def fill_dict(self, obj):
        for val in ["SpecimenTypeCV", "SiteTypeCV", "CensorCodeCV"]:
            try:
                getattr(obj, val)
            except:
                pass


    def get_new_objects(self, obj, valuedict):

        for key in dir(obj):
            if "obj" in key.lower():  # key.contains("Obj"):
                try:
                    att = getattr(obj, key)

                    objkey = key.replace("Obj", "ID")
                    if att is not None:
                        valuedict[objkey] = self.added_objs[att]
                    else:
                        valuedict[objkey] = None

                except Exception as e:
                    # print ("cannot find {} in {}. Error:{} in YamlPrinter".format(key, obj.__class__, e))
                    pass

                except Exception as e:
                    print e
                    self._session_out.rollback()
        return valuedict


    def get_inherited(self, sess, model, **kwargs):
        uuid = {}
        for key in kwargs.keys():
            if "uuid" in key.lower():
                uuid[key] = kwargs[key]
                break
        try:
            setSchema(self._engine_out)
            if len(uuid) > 0:
                instance = sess.query(model).filter_by(**uuid).first()
            else:
                instance = sess.query(model).filter_by(**kwargs).first()
            return instance
        except:
            return None

    def get_or_create(self, sess, model, **kwargs):
        # instance = sess.query(model).filter_by(**kwargs).first()
        instance = self.get_inherited(sess, model, **kwargs)

        if instance:
            return instance
        else:
            instance = model(**kwargs)
            new_instance = sess.merge(instance)
            sess.flush()
            return new_instance

