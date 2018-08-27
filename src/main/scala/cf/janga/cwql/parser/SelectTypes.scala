package cf.janga.cwql.parser

case class Projection(statistic: Statistic, namespaceAlias: Option[String], alias: Option[String], metric: String)

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
case object Equals extends ComparisonOperator {
  override def toString = "="
}

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
sealed trait Statistic {
  def value: String
}
case object Average extends Statistic {
  override def value: String = "avg"
}
case object Sum extends Statistic {
  override def value: String = "sum"
}
case object Minimum extends Statistic {
  override def value: String = "min"
}
case object Maximum extends Statistic {
  override def value: String = "max"
}
