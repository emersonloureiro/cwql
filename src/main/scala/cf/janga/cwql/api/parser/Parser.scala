package cf.janga.cwql.api.parser

import org.parboiled2.{Parser => ParboiledParser, _}

import scala.util.{Failure, Success}

case class ParserError(line: Int, column: Int) extends RuntimeException

class Parser {

  def parse(query: String): Either[ParserError, CwqlStatement] = {
    new InnerParser(query.stripMargin).Sql.run() match {
      case Success(parsedQuery) => Right(parsedQuery)
      case Failure(ParseError(position, principalPosition, traces)) => {
        println(principalPosition)
        Left(ParserError(principalPosition.line, principalPosition.column))
      }
    }
  }
}

private class InnerParser(val input: ParserInput) extends ParboiledParser {

  def Sql = rule {
    NonRequiredSpaceRule ~ SelectRule ~ RequiredSpaceRule ~ FromRule ~ RequiredSpaceRule ~ optional(WhereRule ~ RequiredSpaceRule) ~ BetweenRule ~ RequiredSpaceRule ~ PeriodRule ~ NonRequiredSpaceRule ~ EOI ~> {
      (select, namespaces, where, between, period) => {
        Query(select, namespaces, where.asInstanceOf[Option[Selection]], between, period)
      }
    }
  }

  def PeriodRule = rule {
    ignoreCase("period") ~ RequiredSpaceRule ~ IntegerRule ~> Period
  }

  def BetweenRule = rule {
    ignoreCase("between") ~ RequiredSpaceRule ~ TimestampRule ~ RequiredSpaceRule ~ ignoreCase("and") ~ RequiredSpaceRule ~ TimestampRule ~> Between
  }

  def TimestampRule = rule {
    capture(oneOrMore(anyOf("0123456789-:+TZ")))
  }

  def SelectRule = rule {
    ignoreCase("select") ~ RequiredSpaceRule ~ oneOrMore(ProjectionStatisticRule).separatedBy(NonRequiredSpaceRule ~ str(",") ~ NonRequiredSpaceRule)
  }

  def IdentifierRule = rule {
    capture(oneOrMore(anyOf("AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwYyXxZz0123456789_")))
  }

  def ProjectionStatisticRule = rule {
    StatisticsRule ~ ch('(') ~ optional(IdentifierRule ~ ch('.')) ~ IdentifierRule ~ ch(')') ~ (optional(RequiredSpaceRule ~ ignoreCase("as") ~ RequiredSpaceRule ~ IdentifierRule) ~> ((alias) => alias.asInstanceOf[Option[String]])) ~> ((statistic, namespaceAlias, metric, alias) => Projection(statistic, namespaceAlias.asInstanceOf[Option[String]], alias.asInstanceOf[Option[String]], metric))
  }

  def StatisticsRule = rule {
    (capture(ignoreCase("avg")) | capture(ignoreCase("sum")) | capture(ignoreCase("max")) | capture(ignoreCase("min"))) ~> ((statistic) => Statistic(statistic))
  }

  def NamespacesRule = rule {
    oneOrMore(capture(oneOrMore(anyOf("AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwYyXxZz0123456789_/")))
      ~ optional(RequiredSpaceRule ~ ignoreCase("as") ~ RequiredSpaceRule ~ IdentifierRule) ~> ((namespace, alias) => (namespace, alias))).separatedBy(NonRequiredSpaceRule ~ str(",") ~ NonRequiredSpaceRule) ~>
      (namespaces => namespaces.map(namespace => Namespace(namespace._1, namespace._2.asInstanceOf[Option[String]])))
  }

  def FromRule = rule {
    ignoreCase("from") ~ RequiredSpaceRule ~ NamespacesRule
  }

  def WhereRule = rule {
    ignoreCase("where") ~ RequiredSpaceRule ~ BooleanExpressionRule ~> Selection
  }

  def BooleanExpressionRule = rule {
    SimpleBooleanExpressionRule ~ zeroOrMore(RequiredSpaceRule ~ BooleanOperatorRule ~ RequiredSpaceRule ~ SimpleBooleanExpressionRule ~> ((rule, expression) => (rule, expression))) ~> ((rule: SimpleBooleanExpression, nestedExpressions) => BooleanExpression(rule, nestedExpressions.asInstanceOf[Seq[(BooleanOperator, SimpleBooleanExpression)]]))
  }

  def BooleanOperatorRule = rule {
    capture(ignoreCase("and")) ~> (booleanOperator => BooleanOperator(booleanOperator))
  }

  def SimpleBooleanExpressionRule = rule {
    optional(IdentifierRule ~ ch('.')) ~ IdentifierRule ~ NonRequiredSpaceRule ~ ComparisonOperatorRule ~ NonRequiredSpaceRule ~ ValueRule ~> ((alias, identifier, comparison, value) => SimpleBooleanExpression(alias.asInstanceOf[Option[String]], identifier, comparison, value))
  }

  def ValueRule = rule {
    ConstantRule
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
    ('\'' ~ NonRequiredSpaceRule ~ capture(zeroOrMore(!'\'' ~ ANY)) ~ NonRequiredSpaceRule ~ '\'') ~> (a => StringValue(a))
  }

  def RequiredSpaceRule = rule {
    oneOrMore(anyOf(" \t \n"))
  }

  def NonRequiredSpaceRule = rule {
    zeroOrMore(anyOf(" \t \n"))
  }
}
