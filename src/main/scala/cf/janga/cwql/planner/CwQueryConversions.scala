package cf.janga.cwql.planner

import cf.janga.cwql.parser._
import com.amazonaws.services.cloudwatch.model.Dimension

object CwQueryConversions {

  implicit class BooleanExpressionConversion(simpleBooleanExpression: SimpleBooleanExpression) {
    def toDimension: Dimension = {
      val dimension = new Dimension()
      dimension.setName(simpleBooleanExpression.left)
      simpleBooleanExpression.right match {
        case StringValue(stringValue) => dimension.setValue(stringValue)
        case IntegerValue(integerValue) => dimension.setValue(integerValue.toString)
      }
      dimension
    }
  }

  implicit class StatisticConversions(statistic: Statistic) {
    def toAwsStatistic: String = statistic match {
      case Average => "Average"
      case Sum => "Sum"
      case Maximum => "Maximum"
      case Minimum => "Minimum"
    }
  }

}
