package cf.janga.cwql.parser

import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class SelectStatementParsingTest extends WordSpec with Matchers {

  "cwql parser" when {
    "given a select statement" should {
      "parse a single projection" in {
        val queryString = "select avg(time) from requests between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        query.projections should be(Seq(Projection(Statistic("avg"), None, None, "time")))
        query.namespaces should be(Seq(Namespace("requests", None)))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse multiple projections" in {
        val queryString = "select avg(size), avg(time) from requests between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        query.projections should be(Seq(Projection(Statistic("avg"), None, None, "size"), Projection(Statistic("avg"), None, None, "time")))
        query.namespaces should be(Seq(Namespace("requests", None)))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse projections with namespace aliases" in {
        val queryString = "select max(alias1.size), min(alias2.time) from requests between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        query.projections should be(Seq(Projection(Statistic("max"), Some("alias1"), None, "size"), Projection(Statistic("min"), Some("alias2"), None, "time")))
        query.namespaces should be(Seq(Namespace("requests", None)))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse projections with aliases" in {
        val queryString = "select max(size) as size, min(time) as time from requests between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        query.projections should be(Seq(Projection(Statistic("max"), None, Some("size"), "size"), Projection(Statistic("min"), None, Some("time"), "time")))
        query.namespaces should be(Seq(Namespace("requests", None)))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse CW namespace" in {
        val queryString = "select sum(alias1.size), sum(alias2.time) from AWS/EC2 between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        query.projections should be(Seq(Projection(Statistic("sum"), Some("alias1"), None, "size"), Projection(Statistic("sum"), Some("alias2"), None, "time")))
        query.namespaces should be(Seq(Namespace("AWS/EC2", None)))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse CW namespace with alias" in {
        val queryString = "select sum(ec2.size), sum(ec2.time) from AWS/EC2 as ec2 between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        query.projections should be(Seq(Projection(Statistic("sum"), Some("ec2"), None, "size"), Projection(Statistic("sum"), Some("ec2"), None, "time")))
        query.namespaces should be(Seq(Namespace("AWS/EC2", Some("ec2"))))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }
    }

    "given a where clause" should {
      "parse a single boolean expression" in {
        val queryString = "select avg(size), avg(time) from requests where status='200' between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        val Some(selection) = query.selectionOption
        selection.booleanExpression.simpleBooleanExpression should be(SimpleBooleanExpression(None, "status", Equals, StringValue("200")))
        selection.booleanExpression.nested should be(Seq())
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse multiple boolean expressions" in {
        val queryString = "select max(size), sum(time) from requests where status='200' and size = 10 and time = 5 between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        val Some(selection) = query.selectionOption
        selection.booleanExpression.simpleBooleanExpression should be(SimpleBooleanExpression(None, "status", Equals, StringValue("200")))
        selection.booleanExpression.nested should be(Seq(
          (And, SimpleBooleanExpression(None, "size", Equals, IntegerValue(10))),
          (And, SimpleBooleanExpression(None, "time", Equals, IntegerValue(5)))
        ))
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }

      "parse boolean expressions with aliases" in {
        val queryString = "select avg(r.size), avg(r.time) from requests as r where r.status='200' between 2018-01-01T00:00:00Z and 2018-01-31T:23:59:59Z period 10"
        val Right(query: Query) = new Parser().parse(queryString)
        val Some(selection) = query.selectionOption
        selection.booleanExpression.simpleBooleanExpression should be(SimpleBooleanExpression(Some("r"), "status", Equals, StringValue("200")))
        selection.booleanExpression.nested should be(Seq())
        query.between.startTime should be("2018-01-01T00:00:00Z")
        query.between.endTime should be("2018-01-31T:23:59:59Z")
        query.period should be(Period(10))
      }
    }
  }
}
