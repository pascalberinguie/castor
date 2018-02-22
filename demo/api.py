#!/usr/bin/python
from __future__ import division
import json
import time
#import logging
from flask import Flask, request, abort, make_response
from flask_restful import Resource, Api
from castor.config import CastorConfiguration 
from castor.storage import CastorEngine
from castor.castor_api import DataSourcesApi
from castor.castor_api import ApiError
from castor.castor_api import DataPointsApi

CASTOR_CONF='demo/castor.conf'
storage = CastorEngine(CASTOR_CONF)
#cdef that are currently imported from netstat
#avoid to get the same cdef several times

app = Flask(__name__)
api = Api(app)


class DataPointsCollection(Resource):

    def __init__(self):
        Resource.__init__(self)
        config = CastorConfiguration(CASTOR_CONF)
        self.key = None
        if config.has_param('DATAPOINTS_SECRET_KEY'):
            self.key = config['DATAPOINTS_SECRET_KEY']

    def post(self):
        """
        implements POST /datapoints
        json must contain
        cdef_expr: cdef expression to evaluate
        hash: mandatory if DATAPOINTS_SECRET_KEY is set in netstat.conf
        id_host: mandatory only if cdef as floating references on hosts :3200,:3205,+
        stime:
        etime:
        function: AVG,MAX default AVG
        """
        dp_api = DataPointsApi(storage, app.logger)
        try:
            params = json.loads(request.data)
            app.logger.debug(params)
            return dp_api.post(params, self.key), 201
        except ValueError as v:
            abort(406, "Invalid json %s"%request.data)
        except ApiError as e:
            abort(e.code, e.error_str)




class DatasourceCollection(Resource):
    """
    Collection datasource
    """

    def get(self,dsname):
        ds_api = DataSourcesApi(storage, app.logger)
        try:
            result = ds_api.get(dsname)
        except ApiError as e:
            abort(e.code, e.error_str)
        return result, 200 

    
    def post(self,dsname):
        ds_api = DataSourcesApi(storage, app.logger)
        try:
            params = json.loads(request.data)
            app.logger.debug(params)
            ds_api.post(dsname, params)
            return {'success': True, 'id': params['ds_name']}, 201, {'Location':'/'}
        except ValueError:
            abort(406, "Can't read json values")
        except ApiError as e:
            abort(e.code, e.error_str)


api.add_resource(DataPointsCollection, '/datapoints')
api.add_resource(DatasourceCollection, '/datasources/<dsname>')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
    #app.logger.setLevel(logging.DEBUG)
