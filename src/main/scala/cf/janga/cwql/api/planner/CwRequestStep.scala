package cf.janga.cwql.api.planner

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Datapoint, GetMetricStatisticsRequest, GetMetricStatisticsResult}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import cf.janga.cwql.api.planner.CwQueryConversions._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

private case class StatisticDatapoint(cloudWatchStatistic: String, value: Double, unorderedProjection: UnorderedProjection)
case class GetMetricStatisticsRequestForProjections(cloudWatchRequest: GetMetricStatisticsRequest, projections: Seq[UnorderedProjection])

case class CwRequestStep(awsCredentialsProvider: AWSCredentialsProvider, requests: Seq[GetMetricStatisticsRequestForProjections]) extends Step {

  private val cwClient = AmazonCloudWatchClientBuilder.standard().withCredentials(awsCredentialsProvider).build()

  override def execute(inputOption: Option[ResultSet]): Either[ExecutionError, ResultSet] = {
    doExecute(requests, new HashJoin())
  }

  @tailrec
  private def doExecute(requests: Seq[GetMetricStatisticsRequestForProjections], hashJoin: HashJoin): Either[ExecutionError, ResultSet] = requests match {
    case request :: remaining => {
      val cloudWatchRequest = request.cloudWatchRequest
      callCloudWatch(cloudWatchRequest) match {
        case Right(result) => {
          val datapoints = result.getDatapoints
          datapoints.asScala.foreach {
            datapoint => {
              val timestamp = new DateTime(datapoint.getTimestamp).toString(ISODateTimeFormat.dateTimeNoMillis())
              getStatisticDatapoints(datapoint, request.projections).foreach {
                statisticDatapoint => {
                  val order = statisticDatapoint.unorderedProjection.order
                  val statisticEntryName = statisticDatapoint.unorderedProjection.originalProjection.alias.fold(s"${statisticDatapoint.unorderedProjection.originalProjection.statistic.value}_${cloudWatchRequest.getMetricName}")(alias => alias)
                  val record = Record(timestamp, Map(statisticEntryName -> (statisticDatapoint.value.toString, order)))
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

  private def getStatisticDatapoints(datapoint: Datapoint, unorderedProjections: Seq[UnorderedProjection]): (Seq[StatisticDatapoint]) = {
    case class CollectedStatistic(cloudWatchStatistic: String, projectionStatistic: String, value: Double)
    val averageOption = Option(datapoint.getAverage).map(CollectedStatistic(Constants.Average, "avg", _))
    val minOption = Option(datapoint.getMinimum).map(CollectedStatistic(Constants.Minimum, "min", _))
    val maxOption = Option(datapoint.getMaximum).map(CollectedStatistic(Constants.Maximum, "max", _))
    val sumOption = Option(datapoint.getSum).map(CollectedStatistic(Constants.Sum, "sum", _))
    val collectedStatistics = Seq(averageOption, minOption, maxOption, sumOption).flatten

    for {
      collectedStatistic <- collectedStatistics
      unorderedProjection <- unorderedProjections
      if collectedStatistic.cloudWatchStatistic == unorderedProjection.originalProjection.statistic.toAwsStatistic
    } yield {
      StatisticDatapoint(collectedStatistic.cloudWatchStatistic, collectedStatistic.value, unorderedProjection)
    }
  }
}