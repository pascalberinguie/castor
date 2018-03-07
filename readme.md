# Why castor
Castor in not a graphing tool but a python library that can be plugged to your graphing tool to store datas in cassandra.
A lot of old graphing tools use rrdtool (https://oss.oetiker.ch/rrdtool/) that is a solution that store data in binary files and enables you to create graphs based on evaluation of rpn expression composed of datasources previously stored
Some modern tools use more scalable storage solutions like cassandra but don't permit to make calculations based on several stored datasources.
Castor is a pragmatic solution that store data in cassandra and enables you to request evaluations of rpn expression.
So it may be easy to adapt your graphing tool based on rrdtool to use Castor. Castor don't generate graphs as png image but only provide an api to request values because a lot of modern solutions enables you to produce graphs on client side (like https://www.highcharts.com/ for example)

# Start with castor

## Test with web api:

* Configure demo/castor.conf
* Run in a terminal
```
python demo/api.py
```

* In another terminal:

```
#to post values for a variable_name 'testapi' that will be created if not exists

curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{"ds_infos":"test via api", "values_type":"gauge", "values":{"1515167463":8,"1515167493":16}}' 'http://127.0.0.1:8000/datasources/testapi'

{
    "id": "testapi", 
    "success": true
}

```

* variable has been created

```
curl http://127.0.0.1:8000/datasources/testapi

{
    "computed_retention": 158112000, 
    "ds_infos": "test via api", 
    "ds_name": "testapi", 
    "first_raw": null, 
    "last_agregated": null, 
    "last_inserted_ts": 1515167493, 
    "raw_retention": 34819200
}

```

* to post values for another variable

```
curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{"ds_infos":"test via api", "values_type":"gauge", "values":{"1515167462":10,"1515167496":30}}' 'http://127.0.0.1:8000/datasources/testapi2'
{
    "id": "testapi2", 
    "success": true
}
```

* to request values corresponding to (testapi1+testapi2)*10

```
curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{"cdef_expr": "testapi,testapi2,+,10,*","stime":1515167460,"etime":1515167500}' 'http://127.0.0.1:8000/datapoints'

{
    "AVG": [
        [
            1515167460000, 
            180.0
        ], 
        [
            1515167490000, 
            460.0
        ], 
        [
            1515167500000, 
            null
        ]
    ], 
    "cdef": "testapi,testapi2,+,10,*", 
    "etime": 1515167500, 
    "stime": 1515167460
}
```

### How to secure calls to api
If you want to limt calls to your api, yo can declare a variable 
```
DATAPOINTS_SECRET_KEY=MyPasword
```
in castor conf file. And a parameter hash will be required in json to POST on datapoints collection
with hash=md5(rpn expr + "," + DATAPOINTS_SECRET_KEY)


## Tests using python api:

```python
export CONF_FILE='path/to/your/conf/file'
python  demo/demo.py

#demo.py is a simple script that contains lines:
ts = int(time.time()) - 1000

storage = CastorEngine(CONF_FILE)
        
storage.insert_collected_values('var1', 'g', {ts+10 : 5.0, ts+20: 10.0, ts+30: 20.5, ts+40: 16}, use_batch=False)
storage.insert_collected_values('var2', 'g', {ts+10 : 100, ts+20: 15.0, ts+30: 22, ts+40:1}, use_batch=False)
print storage.eval_cdef('var1,var2,+,10,*', ts, ts+4000, 10)
```


# Implementation

## Tables in cassandra
castor stores values in cassandra column families in the keyspace specified in castor.conf file

```plantuml
class metadatas{
str ds_name
str ds_infos
int raw_retention
int computed_retention
int first_raw
int last_agregated
int last_inserted_ts

}

class raw_data{
str ds_name
int period
int timestamp
str tag
int counter
int gauge    
}


class daily_data{
str ds_name
int period
int timestamp 
str tag 
float avg
float max
}

class weekly_data{
str ds_name
int period
int timestamp 
str tag 
float avg
float max
}

class monthly_data{
str ds_name
int period
int timestamp 
str tag 
float avg
float max
}

class yearly_data{
str ds_name
int period
int timestamp 
str tag 
float avg
float max
}
```

* Table raw_data contains collected values whithout any transformation. If value is a gauge, it's stored in colum gauge. If value is a counter, it's stored un colum counter.
Patition key is composed of ds_name and period. Period is the number of day since 1st january 1970 int(timestamp/86400)
* Other tables  daily_data, weekly_data, monthly_data, yearly_data contains values computed by function storage.archive_ds with greater steps. 
    * In table daily_data we store a value each 5 minutes. 
    * In table weekly_data we store a value each 50 minutes
    * In table monthly_data we store a value each 3h20 minutes
    * In table yearly_data we store a value each 40h

This steps can be changed directly in CastorEngine constructor (storage.py) but you can't make this operation if your database is not empty.
Field period (used in partition key) is the number of day since 1970 for daily_data int(timestamp/number of seconds in a day), the number of week since 1970 for weekly_data int(timestamp/number of seconds in a week) etc...
Function archive_ds take all values in raw_data table in range ts-required_step/2 - ts+required_step/2 and store in destination table the max and the average. 

When we request values, castor algorithm read datas in the less precise table that give a sufficient precision.
For example if you want 200 points on your graph for a period of one year. A point each 40H is sufficient. So Castor will read only in table yearly_data for values older than valued contained in field last_agregated of table metadatas



* Table metadatas contains informations about datasource identified by ds_name
    * ds_infos: datasource description
    * raw_retention: time in seconds in which raw values (in table raw_data) are kept
    * computed_retention: time in seconds in which computed values (tables daily_data, weekly_data, monthly_data, yearly_data) are kept
    * first_raw: oldest timestamp in raw data for datasource
    * last_agregated: most recent value computed in tables daily_data, weekly_data, etc.... (last time function archive_ds has been called on this datasource) It's necessary to read values in raw_datas table if most recent values are needed
    * last_inserted_ts: most recent value in raw data. This value is not required by castor  and is set only if values are inserted by datasource api
    
# Code 

## castor.config
```plantuml
class CastorConfiguration{
    def __get_item__()
}
class CassandraCF{
    self.colum_family
    self.session
}
```
* Class CastorConfiguration enables you to read a conf file like
```
PARAM1 = V1
PARAM2 = V2
```
as a dict()

* Class CassandraKeyspace initialize a connection to cassandra and you can inherit from it to use self.session

## castor.rrd
```plantuml
CassandraKeyspace <|-- RRDBaseStorage
RRDBaseStorage <|-- RRDRawStorage
RRDBaseStorage <|-- RRDAgregatedStorage

class RRDBaseStorage{
    def get_rows()
    def execute_write_request()
}


class RRDRawStorage{
    def insert_gauge()
    def insert_counter()
    def get_rows()
}

class RRDAgregatedStorage{
    def insert_agregated_values()
    def delete_agregated_values()
    def get_rows()
}

class Dataset{
    def __discretize__()
    def interpolate()
}
```
* Class RRDBaseStorage inherits from column family and represents a CF that stores data
* Class RRDRawStorage inherits from RRDBaseStorage and manages table raw_data. Most important method is get_rows to get datas for a datasource between start and end timestamp
start and end must be in the same day GMT (field period in table (nb of day since 1970)) or a TooLongPeriodException is raised. A Dataset is returned
* Class RRDAgregatedStorage manages tables daily_data, weekly_data, etc.... Like RRDRawStorage, it provides a method get_rows that returns a Dataset contained your values. start_time and end_time must belong to the same period.
week since 1970 for weekly_datas, month since 1970 for monthly_data,etc...
* Class Dataset: If parameter wanted_step is passed to constructor, datas are discretized. We keep only a point (wanted_point) each wanted step if datas are present between wanted_point-(step/2) wanted_point+(step/2)
If you want to compute an expression based on several datasources with different steps, you may have to call method interpolate that used Taylor Young formula to add new points between discretized values

## castor.metadata
```plantuml
class MetaData
CassandraKeyspace <|-- MetaDataStorage
```
* Classes MetaData and MetaDataStorage manage table metadatas and enables you to get or to store values in this table concerning your datasources

## castor.operations
```plantuml
class Operation{
def __init__(self, rpn_expr)
def set_dataset(self,name, dataset)
def evaluate(self, timestamp, consolidation_func='AVG')
}
```
You call constructor with your rpn expression. For each variable contained in your expression you associate a Dataset with method set_dataset(). And foreach timestamp, you call function evaluate to get data.
Note that method interpolate is called on each Dataset to have points for the same timestamps between datasources to make evaluation possible

If you want to modify class to manage an other types of operations, it is very simple. Define your operation and complete dict "operations" 

```python
operations = { '+': add,'-': minus,...


 def add(stack):
        x = stack.pop()
        y = stack.pop()
        return x+y

def minus(stack):
        x = stack.pop()
        y = stack.pop()
        return y-x
```

## castor.storage
```plantuml

class CastorEngine{
def insert_collected_values(self, ds_name, vtype, values, tag='', use_batch=False, update_last_inserted_value=False)
def update_or_create_metadata(self, ds_name, first_raw=None, last_agregated=None, raw_retention=None, computed_retention=None, ds_infos=None, last_inserted_ts=None):
def get_metadata_by_name(self, ds_name):
def eval_cdef(self, expr_cdef, starttime, endtime, step, consolidation=None, tag=None, raw_data_allowed = True)
def archive_ds(self, ds_name, full_archive=False, end_date=None)
def get_best_step(self, start, end, needed_points=250)
def get_report_on_agregated_data(self, ds_name, ts)
  
}

```
It's the higher level class that you will use if you want to call Castor directly in Python. 
* insert_collected_values: to store values as raw datas. vtype expect g or c (gauge or counter) A tag can be specified if you want to filter on some values having the same tag.
This option is not tested yet and will be fully available in futures releases. Option update_last_inserted_value (false by default) set the timestamp of the last inserted value in metadatas. Use this option only if you need to request this field because it makes additional writes to cassandra
* archive_ds(ds_name) fill tables daily_data, monthly_data, etc... You may called it each day on each ds. If you dont do that, Castor will read only table raw_datas each time you request values and performances will be catastrophic if you request long periods
* eval_cdef: To request values for a rpn expression during a period.
* get_best_step: Will give you the best step to pass to method eval_cdef taking in account the value you pass for needed_points. The step that will be given not correspond strictly to your value for needed_points but to the nearest value given good performances

**See examples above** to use these functions

# castor.castor_api
```plantuml

CastorApi <|-- DataPointsApi
CastorApi <|-- DataSourcesApi

class DataPointsApi{
    def post
}

class DataSourcesApi{
   def get
   def post
}

```
Classes DataPointsApi and DataPointsApi implements web api
