package cf.janga.cwql.api.parser

case class MetricData(metricName: String, metricUnit: String, value: String)

case class MetricDimension(name: String, value: String)
