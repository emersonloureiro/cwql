package cf.janga.cwql.parser

case class MetricData(metricName: String, metricUnit: String, value: String)

case class MetricDimension(name: String, value: String)
