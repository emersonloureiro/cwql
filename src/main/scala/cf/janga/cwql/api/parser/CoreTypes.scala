package cf.janga.cwql.api.parser

sealed trait CwqlStatement

case class Query(projections: Seq[Projection],
                 namespaces: Seq[Namespace],
                 selectionOption: Option[Selection],
                 between: Between, period: Period) extends CwqlStatement

case class Insert(namespace: Namespace,
                  metricData: Seq[MetricData],
                  dimensions: Seq[MetricDimension]) extends CwqlStatement
