package cf.janga.sqlparser

case class Query(projection: Projection, from: From, selectionOption: Option[Selection])

case class Projection(values: Seq[String])

case class From(values: Seq[String])

case class Selection(booleanExpression: BooleanExpression)

case class BooleanExpression(simpleBooleanExpression: SimpleBooleanExpression, nested: Seq[(BooleanOperator, SimpleBooleanExpression)])

case class BooleanOperator(operator: String)

case class SimpleBooleanExpression(left: String, operator: ComparisonOperator, right: Value)

case class ComparisonOperator(s: String)

sealed trait Value
case class StringValue(value: String) extends Value
case class IntegerValue(value: Int) extends Value
case class IdentifierValue(value: String) extends Value