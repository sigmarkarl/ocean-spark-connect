## Wrapper to create Spark Connect session for Applications in Ocean

```
spark = OceanSparkSession.Builder().cluster_id("osc-cluster").appid("appid").profile("default").getOrCreate()
spark.sql("select random()").show()
spark.stop()
```

### Options for OceanSparkSession.Builder with and without default values

token
profile
appId
accountId
clusterId
host = "api.spotinst.io"
port = "15002"
bindAddress = "0.0.0.0"