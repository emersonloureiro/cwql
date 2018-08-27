package cf.janga.cwql.planner

import cf.janga.cwql.planner.CwQueryConversions._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.model.{Datapoint, GetMetricStatisticsRequest}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.annotation.tailrec
import scala.collection.JavaConverters._

private case class StatisticDatapoint(cloudWatchStatistic: String, value: Double, unorderedProjection: UnorderedProjection)
case class GetMetricStatisticsRequestForProjections(cloudWatchRequest: GetMetricStatisticsRequest, projections: Seq[UnorderedProjection])

case class GetMetricStatisticsStep(awsCredentialsProvider: AWSCredentialsProvider, requests: Seq[GetMetricStatisticsRequestForProjections])
  extends Step with CwStep {

  override def execute(inputOption: Option[Result]): Either[ExecutionError, Result] = {
    doExecute(requests, new HashJoin())
  }

  @tailrec
  private def doExecute(requests: Seq[GetMetricStatisticsRequestForProjections], hashJoin: HashJoin): Either[ExecutionError, Result] = requests match {
    case request :: remaining => {
      val cloudWatchRequest = request.cloudWatchRequest
      callCloudWatch(cwClient => cwClient.getMetricStatistics(cloudWatchRequest)) match {
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