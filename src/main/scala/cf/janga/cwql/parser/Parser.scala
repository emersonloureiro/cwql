package cf.janga.cwql.parser

import org.parboiled2.{Parser => ParboiledParser}
import org.parboiled2._

import scala.util.Try

class Parser {

  def parse(query: String): Try[CwQuery] = {
    new InnerParser(query.stripMargin).Sql.run()
  }
}

private class InnerParser(val input: ParserInput) extends ParboiledParser {

  def Sql = rule {
    WSRule ~ SelectRule ~ WSRule ~ FromRule ~ WSRule ~ optional(WhereRule) ~ WSRule ~ BetweenRule ~ WSRule ~ PeriodRule ~ WSRule ~ EOI ~> {
      (select, namespaces, where, between, period) => {
        CwQuery(select, namespaces, where.asInstanceOf[Option[Selection]], between, period)
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
    ignoreCase("select") ~ WSRule ~ oneOrMore(ProjectionStatisticRule).separatedBy(WSRule ~ str(",") ~ WSRule)
  }

  def IdentifierRule = rule {
    capture(oneOrMore(anyOf("AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwYyXxZz0123456789_")))
  }

  def ProjectionStatisticRule = rule {
    StatisticsRule ~ WSRule ~ ch('(') ~ WSRule ~ optional(IdentifierRule ~ ch('.')) ~ IdentifierRule ~ WSRule ~ ch(')') ~> ((statistic, alias, id) => Projection(statistic.asInstanceOf[Statistic], alias.asInstanceOf[Option[String]], id.asInstanceOf[String]))
  }

  def StatisticsRule = rule {
    (capture(ignoreCase("avg")) | capture(ignoreCase("sum")) | capture(ignoreCase("max")) | capture(ignoreCase("min"))) ~> ((statistic) => Statistic(statistic))
  }

  def NamespacesRule = rule {
    oneOrMore(capture(oneOrMore(anyOf("AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwYyXxZz0123456789_/")))).separatedBy(WSRule ~ str(",") ~ WSRule) ~>
      (namespaces => namespaces.map(Namespace))
  }

  def FromRule = rule {
    ignoreCase("from") ~ WSRule ~ NamespacesRule
  }

  def WhereRule = rule {
    ignoreCase("where") ~ WSRule ~ BooleanExpressionRule ~> Selection
  }

  def BooleanExpressionRule = rule {
    SimpleBooleanExpressionRule ~ zeroOrMore(WSRule ~ BooleanOperatorRule ~ WSRule ~ SimpleBooleanExpressionRule ~ WSRule ~> ((rule, expression) => (rule, expression))) ~> ((rule: SimpleBooleanExpression, nestedExpressions) => BooleanExpression(rule, nestedExpressions.asInstanceOf[Seq[(BooleanOperator, SimpleBooleanExpression)]]))
  }

  def BooleanOperatorRule = rule {
    capture(ignoreCase("and")) ~> (booleanOperator => BooleanOperator(booleanOperator))
  }

  def SimpleBooleanExpressionRule = rule {
    IdentifierRule ~ WSRule ~ ComparisonOperatorRule ~ WSRule ~ ValueRule ~> SimpleBooleanExpression
  }

  def ValueRule = rule {
    (ConstantRule)
  }

  def ComparisonOperatorRule = rule {
    capture("=") ~> (operator => ComparisonOperator(operator))
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
