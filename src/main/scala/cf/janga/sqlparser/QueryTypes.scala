package cf.janga.sqlparser

case class Query(projection: Projections, from: From, selectionOption: Option[Selection])

case class Projections(values: Seq[Projection])

case class From(values: Seq[String])

case class Selection(booleanExpression: BooleanExpression)

case class BooleanExpression(simpleBooleanExpression: SimpleBooleanExpression, nested: Seq[(BooleanOperator, SimpleBooleanExpression)])

case class BooleanOperator(operator: String)

case class SimpleBooleanExpression(left: String, operator: ComparisonOperator, right: Value)

case class ComparisonOperator(s: String)

case class Projection(alias: Option[String], value: String)

sealed trait Value
case class StringValue(value: String) extends Value
case class IntegerValue(value: Int) extends Value
case class IdentifierValue(identifier: String) extends Value