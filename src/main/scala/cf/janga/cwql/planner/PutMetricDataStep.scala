package cf.janga.cwql.planner

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResult}

case class PutMetricDataStep(awsCredentialsProvider: AWSCredentialsProvider, putMetricDataRequest: PutMetricDataRequest)
  extends Step with CwStep {

  override def execute(inputOption: Option[Result]): Either[ExecutionError, Result] = {
    callCloudWatch[PutMetricDataResult](cwClient => cwClient.putMetricData(putMetricDataRequest)).flatMap {
      result => {
        result.getSdkHttpMetadata.getHttpStatusCode match {
          case 200 => Right(InsertResult(true))
          case statusCode if (statusCode >= 400 && statusCode < 500) => Left(ClientError)
          case _ => Left(UnknownCwError)
        }
      }
    }
  }
}