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
    (SelectStatement | InsertStatement) ~ EOI
  }

  // Insert statement

  def InsertStatement = rule {
    NonRequiredSpaceRule ~ ignoreCase("insert") ~ RequiredSpaceRule ~ ignoreCase("into") ~ RequiredSpaceRule ~ NamespaceRule ~ RequiredSpaceRule ~ ignoreCase("values") ~ RequiredSpaceRule ~ oneOrMore(ValuesRule).separatedBy(ch(',') ~ NonRequiredSpaceRule) ~ RequiredSpaceRule ~ ignoreCase("with") ~ RequiredSpaceRule ~ zeroOrMore(DimensionRule).separatedBy(',' ~ NonRequiredSpaceRule) ~> ((namespace, values, dimensions) => Insert(Namespace(namespace, None), values, dimensions.asInstanceOf[Seq[MetricDimension]]))
  }

  def DimensionRule = rule {
    IdentifierRule ~ NonRequiredSpaceRule ~ ch('=') ~ NonRequiredSpaceRule ~ DimensionValueRule ~> MetricDimension
  }

  def ValuesRule = rule {
    ch('(') ~ NonRequiredSpaceRule ~ IdentifierRule ~ NonRequiredSpaceRule ~ ch(',') ~ NonRequiredSpaceRule ~ UnitRule ~ NonRequiredSpaceRule ~ ch(',') ~ NonRequiredSpaceRule ~ IdentifierRule ~ NonRequiredSpaceRule ~ ch(')') ~> MetricData
  }

  def UnitRule = rule {
    capture(str("Bytes/Second")) | capture(str("Kilobytes/Second")) | capture(str("Megabytes/Second")) | capture(str("Gigabytes/Second")) | capture(str("Terabytes/Second")) | capture(str("Bits/Second")) | capture(str("Kilobits/Second")) | capture(str("Megabits/Second")) | capture(str("Gigabits/Second")) | capture(str("Terabits/Second")) | capture(str("Count/Second")) | capture(str("Seconds")) | capture(str("Microseconds")) | capture(str("Milliseconds")) | capture(str("Bytes")) | capture(str("Kilobytes")) | capture(str("Megabytes")) | capture(str("Gigabytes")) | capture(str("Terabytes")) | capture(str("Bits")) | capture(str("Kilobits")) | capture(str("Megabits")) | capture(str("Gigabits")) | capture(str("Terabits")) | capture(str("Percent")) | capture(str("Count")) | capture(str("None"))  }

  // Select statement

  def SelectStatement = rule {
    NonRequiredSpaceRule ~ SelectRule ~ RequiredSpaceRule ~ FromRule ~ RequiredSpaceRule ~ optional(WhereRule ~ RequiredSpaceRule) ~ BetweenRule ~ RequiredSpaceRule ~ PeriodRule ~ NonRequiredSpaceRule ~> {
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

  def NamespaceRule = rule {
    capture(oneOrMore(anyOf("AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwYyXxZz0123456789_/")))
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

  def DimensionValueRule = rule {
    NonRequiredSpaceRule ~ capture(zeroOrMore(!'\'' ~ !',' ~ ANY)) ~ NonRequiredSpaceRule
  }

  def RequiredSpaceRule = rule {
    oneOrMore(anyOf(" \t \n"))
  }

  def NonRequiredSpaceRule = rule {
    zeroOrMore(anyOf(" \t \n"))
  }
}
