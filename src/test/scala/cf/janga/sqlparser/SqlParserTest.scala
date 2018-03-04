package cf.janga.sqlparser

import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class SqlParserTest extends WordSpec with Matchers {

  "sql parser" when {
    "given a basic query" should {
      "parse a single projection" in {
        val queryString = "select status from requests"
        val Success(query) = SqlParser.parse(queryString)
        query.projection.values should be(Seq(Projection(None, "status")))
        query.from.values should be(Seq("requests"))
      }

      "parse multiple projections" in {
        val queryString = "select status, time from requests"
        val Success(query) = SqlParser.parse(queryString)
        query.projection.values should be(Seq(Projection(None, "status"), Projection(None, "time")))
        query.from.values should be(Seq("requests"))
      }

      "parse projections with aliases" in {
        val queryString = "select alias1.status, alias2.time from requests"
        val Success(query) = SqlParser.parse(queryString)
        query.projection.values should be(Seq(Projection(Some("alias1"), "status"), Projection(Some("alias2"), "time")))
        query.from.values should be(Seq("requests"))
      }
    }

    "given a where clause" should {
      "parse a single boolean expression" in {
        val queryString = "select status, time from requests where status='200'"
        val Success(query) = SqlParser.parse(queryString)
        val Some(selection) = query.selectionOption
        selection.booleanExpression.simpleBooleanExpression should be(SimpleBooleanExpression("status", ComparisonOperator("="), StringValue("200")))
        selection.booleanExpression.nested should be(Seq())
      }

      "parse multiple boolean expressions" in {
        val queryString = "select status, time from requests where status='200' and size < 10 or time > 5"
        val Success(query) = SqlParser.parse(queryString)
        val Some(selection) = query.selectionOption
        selection.booleanExpression.simpleBooleanExpression should be(SimpleBooleanExpression("status", ComparisonOperator("="), StringValue("200")))
        selection.booleanExpression.nested should be(Seq(
          (BooleanOperator("and"), SimpleBooleanExpression("size", ComparisonOperator("<"), IntegerValue(10))),
          (BooleanOperator("or"), SimpleBooleanExpression("time", ComparisonOperator(">"), IntegerValue(5)))
        ))
      }
    }
  }
}
