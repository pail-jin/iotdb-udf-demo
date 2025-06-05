# iotdb-udf-demo
It's a simple demo for udf using in iotdb.

## How to package

You can get a jar named `iotdb-udf-demo-1.3.1-SNAPSHOT-jar-with-dependencies.jar` under `./target/`
```shell
mvn clean package -pl . -Pget-jar-with-dependencies
```

## Register udf in iotdb using the jar
```
SHOW FUNCTIONS
DROP FUNCTION <UDF-NAME>
```
1. put the jar under `ext/udf`
2. execute the following command in `cli`
```sql
CREATE FUNCTION two_sum AS 'org.apache.iotdb.udf.demo.TwoSum';
```

3. execute `show functions` in `cli`, you can see `two_sum` in the functions list.


CREATE CONTINUOUS QUERY cq_hourly_diff_totalSum
RESAMPLE EVERY 1h RANGE 2h55m, 0m
BEGIN
  SELECT hourly_diff(totalSum) 
  INTO root.sjgd.snnb.::(totalSumHourly)
  FROM root.sjgd.snnb.*
END;