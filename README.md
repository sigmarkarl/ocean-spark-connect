## Wrapper to create Spark Connect session for Spark Applications in Ocean

```
from ocean_spark_connect.ocean_spark_session import OceanSparkSession

spark = OceanSparkSession.Builder().cluster_id("osc-cluster").appid("appid").profile("default").getOrCreate()
spark.sql("select random()").show()
spark.stop()
```

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