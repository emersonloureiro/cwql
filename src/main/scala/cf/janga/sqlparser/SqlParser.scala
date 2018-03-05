package cf.janga.sqlparser

import org.parboiled2._

import scala.util.Try

object SqlParser {

  def parse(query: String): Try[Query] = {
    new SqlParser(query).Sql.run()
  }
}

private class SqlParser(val input: ParserInput) extends Parser {

  def Sql = rule {
    WSRule ~ SelectRule ~ WSRule ~ FromRule ~ WSRule ~ optional(WhereRule) ~ WSRule ~ BetweenRule ~ WSRule ~ EOI ~> {
      (select, from, where, between) => {
        Query(select, from, where.asInstanceOf[Option[Selection]], between)
      }
    }
  }

  def BetweenRule = rule {
    ignoreCase("between") ~ WSRule ~ TimestampRule ~ WSRule ~ ignoreCase("and") ~ WSRule ~ TimestampRule ~> Between
  }

  def TimestampRule = rule {
    capture(oneOrMore(anyOf("0123456789-:+TZ")))
  }

  def SelectRule = rule {
    ignoreCase("select") ~ WSRule ~ ProjectionIdentifiersRule ~> Projections
  }

  def IdentifierRule = rule {
    capture(oneOrMore(anyOf("abcdefghijklmnopqrstuvwyxz0123456789_")))
  }

  def ProjectionIdentifierRule = rule {
    optional(IdentifierRule ~ ch('.')) ~ IdentifierRule ~> ((alias, id) => Projection(alias.asInstanceOf[Option[String]], id.asInstanceOf[String]))
  }

  def ProjectionIdentifiersRule = rule {
    oneOrMore(ProjectionIdentifierRule).separatedBy(WSRule ~ str(",") ~ WSRule)
  }

  def IdentifiersRule = rule {
    oneOrMore(IdentifierRule).separatedBy(WSRule ~ str(",") ~ WSRule)
  }

  def FromRule = rule {
    ignoreCase("from") ~ WSRule ~ IdentifiersRule ~> From
  }

  def WhereRule = rule {
    ignoreCase("where") ~ WSRule ~ BooleanExpressionRule ~> Selection
  }

  def BooleanExpressionRule = rule {
    SimpleBooleanExpressionRule ~ zeroOrMore(WSRule ~ BooleanOperatorRule ~ WSRule ~ SimpleBooleanExpressionRule ~ WSRule ~> ((rule, expression) => (rule, expression))) ~> ((rule: SimpleBooleanExpression, nestedExpressions) => BooleanExpression(rule, nestedExpressions.asInstanceOf[Seq[(BooleanOperator, SimpleBooleanExpression)]]))
  }

  def BooleanOperatorRule = rule {
    (capture(ignoreCase("and")) | capture(ignoreCase("or"))) ~> BooleanOperator
  }

  def SimpleBooleanExpressionRule = rule {
    IdentifierRule ~ WSRule ~ ComparisonOperatorRule ~ WSRule ~ ValueRule ~> SimpleBooleanExpression
  }

  def ValueRule = rule {
    (ConstantRule | IdentifierValueRule)
  }

  def IdentifierValueRule = rule {
    IdentifierRule ~> IdentifierValue
  }

  def ComparisonOperatorRule = rule {
    (capture(">") | capture("<") | capture(">=") | capture("<=") | capture("=") | capture("!=")) ~> ComparisonOperator
  }

  def ConstantRule = rule {
    StringRule | IntegerRule
  }

  def IntegerRule = rule {
    capture(anyOf("123456789") ~ zeroOrMore(anyOf("1234567890"))) ~> (n => IntegerValue(n.toInt))
  }

  def StringRule = rule {
    ('\'' ~ WSRule ~ capture(zeroOrMore(!'\'' ~ ANY)) ~ WSRule ~ '\'') ~> (a => StringValue(a))
  }

  def WSRule = rule {
    zeroOrMore(anyOf(" \t \n"))
  }
}
