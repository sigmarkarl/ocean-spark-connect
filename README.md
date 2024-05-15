# ocean-spark-connect
## Wrapper to create Spark Connect session for Spark Applications in Ocean

```
from ocean_spark_connect.ocean_spark_session import OceanSparkSession

spark = OceanSparkSession.Builder().cluster_id("osc-cluster").appid("appid").profile("default").getOrCreate()
spark.sql("select random()").show()
spark.stop()
```

To use periodic ping to keep the session alive, use the ping_interval option (in seconds). 
The default value is off (-1).

```
spark = OceanSparkSession.Builder() \
    .ping_interval(5.0) \
    .cluster_id("osc-cluster") \
    .appid("appid") \
    .profile("default") \
    .getOrCreate()
```

To use java Spark plugin for the websocket bridge instead, add the use_java(True) option.

### Options for OceanSparkSession.Builder with and without default values

In addition to the existing SparkSession.Builder option, the following options are available:

* token
* profile
* appid
* account_id
* cluster_id
* host = "api.spotinst.io"
* port = "15002"
* bind_address = "0.0.0.0"