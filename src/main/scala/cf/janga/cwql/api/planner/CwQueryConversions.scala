package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser._
import com.amazonaws.services.cloudwatch.model.Dimension

object CwQueryConversions {

  implicit class BooleanExpressionConversion(simpleBooleanExpression: SimpleBooleanExpression) {
    def toDimension(namespace: Namespace, projection: Projection): Option[Dimension] = {
      (namespace.aliasOption, projection.alias, simpleBooleanExpression.alias) match {
        case (Some(namespaceAlias), Some(projectionAlias), Some(booleanExpressionAlias))
          if namespaceAlias == projectionAlias && projectionAlias == booleanExpressionAlias => Some(simpleBooleanExpression.toDimension)
        case ((None, None, None)) => Some(simpleBooleanExpression.toDimension)
        case _ => None
      }
    }

    private def toDimension: Dimension = {
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
