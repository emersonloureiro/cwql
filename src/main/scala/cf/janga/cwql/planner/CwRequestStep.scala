package cf.janga.cwql.planner

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Datapoint, GetMetricStatisticsRequest}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._

case class CwRequestStep(awsCredentialsProvider: AWSCredentialsProvider, requests: Seq[GetMetricStatisticsRequest]) extends Step {

  private val cwClient = AmazonCloudWatchClientBuilder.standard().withCredentials(awsCredentialsProvider).build()

  override def execute(inputOption: Option[ResultSet]): ResultSet = {
    val hashJoin = new HashJoin()
    requests.foreach {
      request => {
        val result = cwClient.getMetricStatistics(request)
        val datapoints = result.getDatapoints
        datapoints.asScala.foreach {
          datapoint => {
            val timestamp = new DateTime(datapoint.getTimestamp).toString(ISODateTimeFormat.dateTimeNoMillis())
            val (statisticName, statisticValue) = getStatistic(datapoint, request)
            val statisticEntryName = s"${statisticName}_${request.getMetricName}"
            val record = Record(Map("timestamp" -> timestamp, statisticEntryName -> statisticValue.toString))
            hashJoin + (timestamp, record)
          }
        }
      }
    }
    ResultSet(hashJoin.result())
  }

  private def getStatistic(datapoint: Datapoint, request: GetMetricStatisticsRequest): (String, Double) = {
    request.getStatistics.asScala.head match {
      case "Average" => ("avg", datapoint.getAverage)
      case "Sum" => ("sum", datapoint.getSum)
      case "Minimum" => ("min", datapoint.getMinimum)
      case "Maximum" => ("max", datapoint.getMaximum)
    }
  }
}