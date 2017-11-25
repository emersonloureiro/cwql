package cf.janga.sqlparser

import org.parboiled2._

import scala.util.Try

object SqlParser {

  def parse(query: String): Try[Query] = {
    new SqlParser(query).Sql.run()
  }
}

private[sqlparser] class SqlParser(val input: ParserInput) extends Parser {

  def Sql = rule {
    WSRule ~ SelectRule ~ WSRule ~ FromRule ~ WSRule ~ optional(WhereRule) ~ WSRule ~ EOI ~> {
      (select, from, where) => {
        Query(select, from, where.asInstanceOf[Option[Selection]])
      }
    }
  }

  def SelectRule = rule {
    ignoreCase("select") ~ WSRule ~ IdentifiersRule ~> Projection
  }

  def IdentifierRule = rule {
    capture(oneOrMore(anyOf("abcdefghijklmnopqrstuvwyxz0123456789_")))
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
