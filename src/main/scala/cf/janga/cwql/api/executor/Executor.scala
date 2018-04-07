package cf.janga.cwql.api.executor

import cf.janga.cwql.api.planner.{ExecutionError, ResultSet, Step}

class Executor {

  def execute(steps: Seq[Step]): Either[ExecutionError, ResultSet] = {
    executeInternal(None, steps)
  }

  private def executeInternal(previousStepOutput: Option[ResultSet], steps: Seq[Step]): Either[ExecutionError, ResultSet] = steps match {
    case nextStep :: remainingSteps => {
      nextStep.execute(previousStepOutput).flatMap {
        stepResult => executeInternal(Some(stepResult), remainingSteps)
      }
    }
    case Nil => Right(previousStepOutput.head)
  }
}
