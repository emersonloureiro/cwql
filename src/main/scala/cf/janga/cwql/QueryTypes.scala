package cf.janga.cwql

case class CwQuery(projection: Projections, from: From, selectionOption: Option[Selection], between: Between, period: Period)

case class Projections(values: Seq[Projection])

case class Projection(statistic: Statistic, alias: Option[String], value: String)

case class From(values: Seq[String])

case class Selection(booleanExpression: BooleanExpression)

case class BooleanExpression(simpleBooleanExpression: SimpleBooleanExpression, nested: Seq[(BooleanOperator, SimpleBooleanExpression)])

case class BooleanOperator(operator: String)

case class SimpleBooleanExpression(left: String, operator: ComparisonOperator, right: Value)

case class ComparisonOperator(s: String)

case class Between(startTime: String, endTime: String)

case class Period(value: Int)

sealed trait Value
case class StringValue(value: String) extends Value
case class IntegerValue(value: Int) extends Value
case class IdentifierValue(identifier: String) extends Value

case class Statistic(value: String)