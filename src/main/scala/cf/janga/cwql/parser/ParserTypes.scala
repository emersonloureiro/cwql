package cf.janga.cwql.parser

case class CwQuery(projections: Seq[Projection], namespaces: Seq[Namespace], selectionOption: Option[Selection], between: Between, period: Period)

case class Projection(statistic: Statistic, alias: Option[String], metric: String)

case class Namespace(value: String)

case class Selection(booleanExpression: BooleanExpression)

case class BooleanExpression(simpleBooleanExpression: SimpleBooleanExpression, nested: Seq[(BooleanOperator, SimpleBooleanExpression)])

object BooleanOperator {
  def apply(operator: String): BooleanOperator = operator match {
    case "and" => And
  }
}
sealed trait BooleanOperator
case object And extends BooleanOperator

case class SimpleBooleanExpression(left: String, comparisonOperator: ComparisonOperator, right: Value)

sealed trait ComparisonOperator
object ComparisonOperator {
  def apply(operator: String): ComparisonOperator = operator match {
    case "=" => Equals
  }
}
case object Equals extends ComparisonOperator

case class Between(startTime: String, endTime: String)

case class Period(value: Int)

sealed trait Value
case class StringValue(value: String) extends Value
case class IntegerValue(value: Int) extends Value

case class Statistic(value: String) {

  def toAws: String = {
    value match {
      case "avg" => "Average"
      case "sum" => "Sum"
      case "max" => "Maximum"
      case "min" => "Minimum"
    }
  }
}
