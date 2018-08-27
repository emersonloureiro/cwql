package cf.janga.cwql.planner

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}

import scala.util.{Failure, Success, Try}

trait CwStep {

  def awsCredentialsProvider: AWSCredentialsProvider

  lazy val cwClient = AmazonCloudWatchClientBuilder.standard().withCredentials(awsCredentialsProvider).build()

  def callCloudWatch[T](f: AmazonCloudWatch => T): Either[ExecutionError, T] = {
    Try(f(cwClient)) match {
      case Success(result) => Right(result)
      case Failure(exception) => Left(CloudWatchClientError(exception.getMessage))
    }
  }
}
