package cf.janga.cwql.planner

import cf.janga.cwql.parser._
import com.amazonaws.services.cloudwatch.model.Dimension

object CwQueryConversions {

  implicit class BooleanExpressionConversion(simpleBooleanExpression: SimpleBooleanExpression) {
    def toDimension(namespace: Namespace, projection: Projection): Option[Dimension] = {
      (namespace.aliasOption, projection.namespaceAlias, simpleBooleanExpression.alias) match {
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
      case Average => Constants.Average
      case Sum => Constants.Sum
      case Maximum => Constants.Maximum
      case Minimum => Constants.Minimum
    }
  }

}
