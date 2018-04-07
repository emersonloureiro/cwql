package cf.janga.cwql.api.planner

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Datapoint, GetMetricStatisticsRequest, GetMetricStatisticsResult}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class DatapointStatistic(statistic: String, value: Double)

case class CwRequestStep(awsCredentialsProvider: AWSCredentialsProvider, requests: Seq[GetMetricStatisticsRequest]) extends Step {

  private val cwClient = AmazonCloudWatchClientBuilder.standard().withCredentials(awsCredentialsProvider).build()

  override def execute(inputOption: Option[ResultSet]): Either[ExecutionError, ResultSet] = {
    doExecute(requests, new HashJoin())
  }

  @tailrec
  private def doExecute(requests: Seq[GetMetricStatisticsRequest], hashJoin: HashJoin): Either[ExecutionError, ResultSet] = requests match {
    case request :: remaining => {
      callCloudWatch(request) match {
        case Right(result) => {
          val datapoints = result.getDatapoints
          datapoints.asScala.foreach {
            datapoint => {
              val timestamp = new DateTime(datapoint.getTimestamp).toString(ISODateTimeFormat.dateTimeNoMillis())
              getStatistics(datapoint, request).foreach {
                datapointStatistic => {
                  val statisticEntryName = s"${datapointStatistic.statistic}_${request.getMetricName}"
                  val record = Record(timestamp, Map(statisticEntryName -> datapointStatistic.value.toString))
                  hashJoin + (timestamp, record)
                }
              }
            }
          }
          doExecute(remaining, hashJoin)
        }
        case Left(error) => Left(error)
      }
    }
    case Nil => Right(ResultSet(hashJoin.result()))
  }

  private def callCloudWatch(request: GetMetricStatisticsRequest): Either[ExecutionError, GetMetricStatisticsResult] = {
    Try(cwClient.getMetricStatistics(request)) match {
      case Success(result) => Right(result)
      case Failure(exception) => Left(CloudWatchClientError(exception.getMessage))
    }
  }

  private def getStatistics(datapoint: Datapoint, request: GetMetricStatisticsRequest): (Seq[DatapointStatistic]) = {
    val averageOption = Option(datapoint.getAverage).map(DatapointStatistic("avg", _))
    val minOption = Option(datapoint.getMinimum).map(DatapointStatistic("min", _))
    val maxOption = Option(datapoint.getMaximum).map(DatapointStatistic("max", _))
    val sumOption = Option(datapoint.getSum).map(DatapointStatistic("sum", _))
    Seq(averageOption, minOption, maxOption, sumOption).flatten
  }
}