package cf.janga.cwql

import org.parboiled2._

import scala.util.Try

object CwqlParser {

  def parse(query: String): Try[Query] = {
    new CwqlParser(query).Sql.run()
  }
}

private class CwqlParser(val input: ParserInput) extends Parser {

  def Sql = rule {
    WSRule ~ SelectRule ~ WSRule ~ FromRule ~ WSRule ~ optional(WhereRule) ~ WSRule ~ BetweenRule ~ WSRule ~ PeriodRule ~ WSRule ~ EOI ~> {
      (select, from, where, between, period) => {
        Query(select, from, where.asInstanceOf[Option[Selection]], between, period)
      }
    }
  }

  def PeriodRule = rule {
    ignoreCase("period") ~ WSRule ~ IntegerRule ~> Period
  }

  def BetweenRule = rule {
    ignoreCase("between") ~ WSRule ~ TimestampRule ~ WSRule ~ ignoreCase("and") ~ WSRule ~ TimestampRule ~> Between
  }

  def TimestampRule = rule {
    capture(oneOrMore(anyOf("0123456789-:+TZ")))
  }

  def SelectRule = rule {
    ignoreCase("select") ~ WSRule ~ ProjectionStatisticsRule ~> Projections
  }

  def IdentifierRule = rule {
    capture(oneOrMore(anyOf("AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwYyXxZz0123456789_")))
  }

  def ProjectionStatisticRule = rule {
    StatisticsRule ~ WSRule ~ ch('(') ~ WSRule ~ optional(IdentifierRule ~ ch('.')) ~ IdentifierRule ~ WSRule ~ ch(')') ~> ((statistic, alias, id) =>Projection(statistic.asInstanceOf[Statistic], alias.asInstanceOf[Option[String]], id.asInstanceOf[String]))
  }

  def StatisticsRule = rule {
    (capture(ignoreCase("avg")) | capture(ignoreCase("sum")) | capture(ignoreCase("max")) | capture(ignoreCase("min"))) ~> Statistic
  }

  def ProjectionStatisticsRule = rule {
    oneOrMore(ProjectionStatisticRule).separatedBy(WSRule ~ str(",") ~ WSRule)
  }

  def NamespacesRule = rule {
    oneOrMore(capture(oneOrMore(anyOf("AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwYyXxZz0123456789_/")))).separatedBy(WSRule ~ str(",") ~ WSRule)
  }

  def FromRule = rule {
    ignoreCase("from") ~ WSRule ~ NamespacesRule ~> From
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
    StringRule | IntegerValueRule
  }

  def IntegerValueRule = rule {
    IntegerRule ~> IntegerValue
  }

  def IntegerRule = rule {
    capture(anyOf("123456789") ~ zeroOrMore(anyOf("1234567890"))) ~> (n => n.toInt)
  }

  def StringRule = rule {
    ('\'' ~ WSRule ~ capture(zeroOrMore(!'\'' ~ ANY)) ~ WSRule ~ '\'') ~> (a => StringValue(a))
  }

  def WSRule = rule {
    zeroOrMore(anyOf(" \t \n"))
  }
}
