package cf.janga.cwql.api.planner

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Datapoint, GetMetricStatisticsRequest}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

case class DatapointStatistic(statistic: String, value: Double)

case class CwRequestStep(awsCredentialsProvider: AWSCredentialsProvider, requests: Seq[GetMetricStatisticsRequest]) extends Step {

  private val cwClient = AmazonCloudWatchClientBuilder.standard().withCredentials(awsCredentialsProvider).build()

  override def execute(inputOption: Option[ResultSet]): Try[ResultSet] = {
    val hashJoin = new HashJoin()
    requests.foreach {
      request => {
        val result = cwClient.getMetricStatistics(request)
        val datapoints = result.getDatapoints
        datapoints.asScala.foreach {
          datapoint => {
            val timestamp = new DateTime(datapoint.getTimestamp).toString(ISODateTimeFormat.dateTimeNoMillis())
            getStatistics(datapoint, request).foreach {
              datapointStatistic => {
                val statisticEntryName = s"${datapointStatistic.statistic}_${request.getMetricName}"
                val record = Record(Map("timestamp" -> timestamp, statisticEntryName -> datapointStatistic.value.toString))
                hashJoin + (timestamp, record)
              }
            }
          }
        }
      }
    }
    Success(ResultSet(hashJoin.result()))
  }

  private def getStatistics(datapoint: Datapoint, request: GetMetricStatisticsRequest): (Seq[DatapointStatistic]) = {
    val averageOption = Option(datapoint.getAverage).map(DatapointStatistic("avg", _))
    val minOption = Option(datapoint.getMinimum).map(DatapointStatistic("min", _))
    val maxOption = Option(datapoint.getMaximum).map(DatapointStatistic("max", _))
    val sumOption = Option(datapoint.getSum).map(DatapointStatistic("sum", _))
    Seq(averageOption, minOption, maxOption, sumOption).flatten
  }
}