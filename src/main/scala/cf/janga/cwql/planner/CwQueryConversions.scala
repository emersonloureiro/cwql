package cf.janga.cwql.planner

import cf.janga.cwql.parser.{IntegerValue, SimpleBooleanExpression, StringValue}
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

}
