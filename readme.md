# Test with web api:

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


# Tests using python api:

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
    

## castor.rrd
```plantuml
class Dataset
CassandraKeyspace <|-- RRDBaseStorage
RRDBaseStorage <|-- RRDRawStorage
RRDBaseStorage <|-- RRDAgregatedStorage
```
