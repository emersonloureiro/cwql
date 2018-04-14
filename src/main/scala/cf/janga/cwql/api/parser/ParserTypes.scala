package cf.janga.cwql.api.parser

case class Query(projections: Seq[Projection], namespaces: Seq[Namespace], selectionOption: Option[Selection], between: Between, period: Period)

case class Projection(statistic: Statistic, alias: Option[String], metric: String)

case class Namespace(value: String, aliasOption: Option[String])

case class Selection(booleanExpression: BooleanExpression)

case class BooleanExpression(simpleBooleanExpression: SimpleBooleanExpression, nested: Seq[(BooleanOperator, SimpleBooleanExpression)])

object BooleanOperator {
  def apply(operator: String): BooleanOperator = operator.toLowerCase match {
    case "and" => And
  }
}
sealed trait BooleanOperator
case object And extends BooleanOperator

case class SimpleBooleanExpression(alias: Option[String], left: String, comparisonOperator: ComparisonOperator, right: Value)

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

object Statistic {
  def apply(value: String): Statistic = value match {
    case "avg" => Average
    case "sum" => Sum
    case "max" => Maximum
    case "min" => Minimum
  }
}
sealed trait Statistic
case object Average extends Statistic
case object Sum extends Statistic
case object Minimum extends Statistic
case object Maximum extends Statistic
