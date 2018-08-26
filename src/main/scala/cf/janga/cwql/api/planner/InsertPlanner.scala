package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser.Insert
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest}

import scala.collection.JavaConverters._

private[planner] object InsertPlanner {

  def planInsert(insert: Insert)(implicit awsCredentialsProvider: AWSCredentialsProvider): Either[PlannerError, CwqlPlan] = {
    val putMetricDataRequest = new PutMetricDataRequest()
    putMetricDataRequest.setNamespace(insert.namespace.value)
    val metricData = getMetricDatumList(insert)
    putMetricDataRequest.setMetricData(metricData.asJava)
    val putMetricDataStep = PutMetricDataStep(awsCredentialsProvider, putMetricDataRequest)
    Right(CwqlPlan(Seq(putMetricDataStep)))
  }

  private def getMetricDatumList(insert: Insert): Seq[MetricDatum] = {
    val cwDimensions = insert.dimensions.map {
      dimension => {
        val cwDimension = new Dimension()
        cwDimension.setName(dimension.name)
        cwDimension.setValue(dimension.value)
        cwDimension
      }
    }.asJava
    insert.metricData.map {
      metricData => {
        val metricDatum = new MetricDatum()
        metricDatum.setMetricName(metricData.metricName)
        // TODO Use double at the parser level
        metricDatum.setValue(metricData.value.toDouble)
        metricDatum.setUnit(metricData.metricUnit)
        metricDatum.setDimensions(cwDimensions)
        metricDatum
      }
    }
  }
}
