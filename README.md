# cwql

SQL-like support on top of the CloudWatch SDK. This is pretty much a work in progress, so there's still limited support.

## Why?

1. It's fun to write parsers and query processors
2. Not a fan of the SDK for publishing and querying metrics
3. SQL is a language anyone is familar with
4. It's flexible in the sense that you can hook up anything you want on top of this (e.g., shell)

## Selecting metrics

```
SELECT
  stat1,
  stat2,
  ...
FROM namespace
WHERE
  dimension1=value1,
  dimension2=value2
BETWEEN start_time AND end_time
PERIOD period
```

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

## Publishing metrics

*Work in progress*
