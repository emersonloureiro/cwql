package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser.{Insert, MetricData, MetricDimension, Namespace}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class InsertionPlanningTest extends WordSpec with Matchers {

  "cqwl planner" when {
    "given an insert statement" should {
      "plan with a single value" in {
        val metricData = Seq(MetricData("testMetric", "Seconds", "1.0"))
        val metricDimensions = Seq(MetricDimension("testDimension", "dimensionValue"))
        val insert = Insert(Namespace("namespace", None), metricData, metricDimensions)

        val Right(insertPlan) = new Planner().plan(insert)
        insertPlan.steps.size should be(1)

        val PutMetricDataStep(_, putMetricDataRequest) = insertPlan.steps.head
        putMetricDataRequest.getNamespace should be("namespace")
        val metricDatumList = putMetricDataRequest.getMetricData.asScala
        metricDatumList.size should be(1)

        val metricDatum = metricDatumList.head
        metricDatum.getMetricName should be("testMetric")
        metricDatum.getUnit should be("Seconds")
        metricDatum.getValue should be(1.0)
        metricDatum.getDimensions.size should be(1)
        val dimension = metricDatum.getDimensions.asScala.head
        dimension.getName should be("testDimension")
        dimension.getValue should be("dimensionValue")
      }

      "plan with multiple values" in {
        val metricData = Seq(MetricData("testMetric", "Seconds", "1.0"), MetricData("anotherTestMetric", "Milliseconds", "1.0"))
        val metricDimensions = Seq(MetricDimension("testDimension", "dimensionValue"))
        val insert = Insert(Namespace("namespace", None), metricData, metricDimensions)
        val Right(insertPlan) = new Planner().plan(insert)
        insertPlan.steps.size should be(1)
        val PutMetricDataStep(_, putMetricDataRequest) = insertPlan.steps.head
        putMetricDataRequest.getNamespace should be("namespace")
        val metricDatumList = putMetricDataRequest.getMetricData.asScala
        metricDatumList.size should be(2)
        val firstMetricDatum = metricDatumList(0)
        firstMetricDatum.getMetricName should be("testMetric")
        firstMetricDatum.getUnit should be("Seconds")
        firstMetricDatum.getValue should be(1.0)
        firstMetricDatum.getDimensions.size should be(1)
        val firstMetricDimension = firstMetricDatum.getDimensions.asScala.head
        firstMetricDimension.getName should be("testDimension")
        firstMetricDimension.getValue should be("dimensionValue")

        val secondMetricDatum = metricDatumList(1)
        secondMetricDatum.getMetricName should be("anotherTestMetric")
        secondMetricDatum.getUnit should be("Milliseconds")
        secondMetricDatum.getValue should be(1.0)
        secondMetricDatum.getDimensions.size should be(1)
        val secondMetricDimension = secondMetricDatum.getDimensions.asScala.head
        secondMetricDimension.getName should be("testDimension")
        secondMetricDimension.getValue should be("dimensionValue")
      }

      "plan with multiple dimensions" in {
        val metricData = Seq(MetricData("testMetric", "Seconds", "1.0"))
        val metricDimensions = Seq(MetricDimension("testDimension", "dimensionValue"), MetricDimension("anotherDimension", "anotherValue"))
        val insert = Insert(Namespace("namespace", None), metricData, metricDimensions)

        val Right(insertPlan) = new Planner().plan(insert)
        insertPlan.steps.size should be(1)

        val PutMetricDataStep(_, putMetricDataRequest) = insertPlan.steps.head
        putMetricDataRequest.getNamespace should be("namespace")
        val metricDatumList = putMetricDataRequest.getMetricData.asScala
        metricDatumList.size should be(1)

        val metricDatum = metricDatumList.head
        metricDatum.getMetricName should be("testMetric")
        metricDatum.getUnit should be("Seconds")
        metricDatum.getValue should be(1.0)
        metricDatum.getDimensions.size should be(2)
        val firstDiimension = metricDatum.getDimensions.asScala(0)
        firstDiimension.getName should be("testDimension")
        firstDiimension.getValue should be("dimensionValue")
        val secondtDiimension = metricDatum.getDimensions.asScala(1)
        secondtDiimension.getName should be("anotherDimension")
        secondtDiimension.getValue should be("anotherValue")
      }
    }
  }
}
