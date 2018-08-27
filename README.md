# cwql

SQL-like support on top of the CloudWatch SDK. Why that? Because...

1. It's fun to write parsers and query processors
2. Not a fan of the SDK for publishing and querying metrics
3. SQL is a language anyone is familar with
4. It's flexible in the sense that you can hook up anything you want on top of this (e.g., shell)

# The Language

## Selecting metrics

```
SELECT
  [nalias.]stat1 [as alias1],
  [nalias.]stat2 [as alias2],
  ...
FROM namespace [as nalias]
WHERE
  [nalias.]dimension1=value1,
  [nalias.]dimension2=value2
BETWEEN start_time AND end_time
PERIOD period
```

where `statn` is a statistic from a metric in the namespace given. Dimensions can be used for filtering also. The start and end times only support the ISO 8601 for now - e.g., `2018-08-01T02:10:38Z` in the UTC time zone. The period is expressed in seconds. Here's an example of a full query:

```
SELECT
 max(CPUUtilization),
 avg(CPUUtilization)
FROM AWS/EC2
WHERE InstanceId='i-13k862a0e1h4673g6'
BETWEEN 2018-03-10T00:00:00Z
 AND 2018-03-10T14:00:00Z
PERIOD 3600
```

Select statements will translate to `GetMetricStatistics` requests to CloudWatch. One request per *metric* will be sent. So in the example above, even though two statistics are being selected, they refer to the same metric, so we can send a single request. A query like the above will return a result set with the `max` and `avg` joined by the timestamp, for example:

| Time | max_CPUUtilization   |      avg_CPUUtilization |
|----------|:-------------:|----------|
| 2018-03-10T:00:00:00Z |  10 | 5 |
| 2018-03-10T:01:00:00Z |  25 | 15 |
| 2018-03-10T:02:00:00Z |  5 | 1 |
...

Note the column names are chosen based on the metric and statistics chosen. You can use aliases also - both for namespaces and the projections - to customize the final column names. So this query

```
SELECT
 max(CPUUtilization) as max_cpu,
 avg(CPUUtilization) as avg_cpu
FROM AWS/EC2 as ec2
WHERE ec2.InstanceId='i-13k862a0e1h4673g6'
BETWEEN 2018-03-10T00:00:00Z
 AND 2018-03-10T14:00:00Z
PERIOD 3600
```
would yield

| Time | max_cpu   |      avg_cpu |
|----------|:-------------:|----------|
| 2018-03-10T:00:00:00Z |  10 | 5 |
| 2018-03-10T:01:00:00Z |  25 | 15 |
| 2018-03-10T:02:00:00Z |  5 | 1 |
...

## Publishing metrics

The general format for publishing metrics is:

```
INSERT INTO namespace VALUES
  (metric_name_1, metric_unit_1, metric_value_1),
  ...
  (metric_name_n, metric_unit_n, metric_value_n),
 WITH
  dimension_name_1=dimension_value_1,
  ...
  dimension_name_n=dimension_value_n
```

For example, the statement below publishes 2 metrics (`latency` and `buffer`) tagged with 2 different dimensions each (`InstanceId` and `InstanceType`):

```
INSERT INTO Instances VALUES
  (latency, Milliseconds, 1234),
  (buffer, Kilobytes/Second, 98765)
 WITH
  InstanceID=123456,
  InstanceType=t2.micro
```

A query like that will translate to a single `PutMetricData` call to CloudWatch. The units have to be one of the units that CloudWatch recognizes, and query will fail at parsing time if that's not the case.
