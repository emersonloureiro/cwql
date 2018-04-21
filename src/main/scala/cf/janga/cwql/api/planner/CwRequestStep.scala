package cf.janga.cwql.api.planner

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.cloudwatch.model.{Datapoint, GetMetricStatisticsRequest, GetMetricStatisticsResult}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

private case class DatapointStatistic(cloudWatchStatistic: String, projectionStatistic: String, value: Double, order: Int)
case class ProjectionsGetMetricStatisticsRequest(cloudWatchRequest: GetMetricStatisticsRequest, projectionStatistics: Seq[ProjectionStatistic])
case class ProjectionStatistic(statistic: String, projectionOrder: Int)

case class CwRequestStep(awsCredentialsProvider: AWSCredentialsProvider, requests: Seq[ProjectionsGetMetricStatisticsRequest]) extends Step {

  private val cwClient = AmazonCloudWatchClientBuilder.standard().withCredentials(awsCredentialsProvider).build()

  override def execute(inputOption: Option[ResultSet]): Either[ExecutionError, ResultSet] = {
    doExecute(requests, new HashJoin())
  }

  @tailrec
  private def doExecute(requests: Seq[ProjectionsGetMetricStatisticsRequest], hashJoin: HashJoin): Either[ExecutionError, ResultSet] = requests match {
    case request :: remaining => {
      val cloudWatchRequest = request.cloudWatchRequest
      callCloudWatch(cloudWatchRequest) match {
        case Right(result) => {
          val datapoints = result.getDatapoints
          datapoints.asScala.foreach {
            datapoint => {
              val timestamp = new DateTime(datapoint.getTimestamp).toString(ISODateTimeFormat.dateTimeNoMillis())
              getStatistics(datapoint, request.projectionStatistics).foreach {
                datapointStatistic => {
                  val statisticEntryName = s"${datapointStatistic.projectionStatistic}_${cloudWatchRequest.getMetricName}"
                  val record = Record(timestamp, Map(statisticEntryName -> (datapointStatistic.value.toString, datapointStatistic.order)))
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

  private def getStatistics(datapoint: Datapoint, projectionStatistics: Seq[ProjectionStatistic]): (Seq[DatapointStatistic]) = {
    val averageOption = Option(datapoint.getAverage).map((Constants.Average, "avg", _))
    val minOption = Option(datapoint.getMinimum).map((Constants.Minimum, "min", _))
    val maxOption = Option(datapoint.getMaximum).map((Constants.Maximum, "max", _))
    val sumOption = Option(datapoint.getSum).map((Constants.Sum, "sum", _))
    val collectedStatistics = Seq(averageOption, minOption, maxOption, sumOption).flatten

    for {
      collectedStatistic <- collectedStatistics
      projectionStatistic <- projectionStatistics
      if collectedStatistic._1 == projectionStatistic.statistic
    } yield {
      DatapointStatistic(projectionStatistic.statistic, collectedStatistic._2, collectedStatistic._3, projectionStatistic.projectionOrder)
    }
  }
}