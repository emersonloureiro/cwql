package cf.janga.sqlparser

import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class CwqlParserTest extends WordSpec with Matchers {

  "sql parser" when {
    "given a basic query" should {
      "parse a single projection" in {
        val queryString = "select status from requests between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Success(query) = CwqlParser.parse(queryString)
        query.projection.values should be(Seq(Projection(None, "status")))
        query.from.values should be(Seq("requests"))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse multiple projections" in {
        val queryString = "select status, time from requests between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Success(query) = CwqlParser.parse(queryString)
        query.projection.values should be(Seq(Projection(None, "status"), Projection(None, "time")))
        query.from.values should be(Seq("requests"))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse projections with aliases" in {
        val queryString = "select alias1.status, alias2.time from requests between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Success(query) = CwqlParser.parse(queryString)
        query.projection.values should be(Seq(Projection(Some("alias1"), "status"), Projection(Some("alias2"), "time")))
        query.from.values should be(Seq("requests"))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse CW namespace" in {
        val queryString = "select alias1.status, alias2.time from AWS/EC2 between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Success(query) = CwqlParser.parse(queryString)
        query.projection.values should be(Seq(Projection(Some("alias1"), "status"), Projection(Some("alias2"), "time")))
        query.from.values should be(Seq("AWS/EC2"))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }
    }

    "given a where clause" should {
      "parse a single boolean expression" in {
        val queryString = "select status, time from requests where status='200' between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Success(query) = CwqlParser.parse(queryString)
        val Some(selection) = query.selectionOption
        selection.booleanExpression.simpleBooleanExpression should be(SimpleBooleanExpression("status", ComparisonOperator("="), StringValue("200")))
        selection.booleanExpression.nested should be(Seq())
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse multiple boolean expressions" in {
        val queryString = "select status, time from requests where status='200' and size < 10 or time > 5 between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Success(query) = CwqlParser.parse(queryString)
        val Some(selection) = query.selectionOption
        selection.booleanExpression.simpleBooleanExpression should be(SimpleBooleanExpression("status", ComparisonOperator("="), StringValue("200")))
        selection.booleanExpression.nested should be(Seq(
          (BooleanOperator("and"), SimpleBooleanExpression("size", ComparisonOperator("<"), IntegerValue(10))),
          (BooleanOperator("or"), SimpleBooleanExpression("time", ComparisonOperator(">"), IntegerValue(5)))
        ))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }
    }
  }
}
