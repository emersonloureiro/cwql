package cf.janga.cwql.executor

import cf.janga.cwql.planner.{ExecutionError, Result, ResultSet, Step}

class Executor {

  def execute(steps: Seq[Step]): Either[ExecutionError, Result] = {
    executeInternal(None, steps)
  }

  private def executeInternal(previousStepOutput: Option[Result], steps: Seq[Step]): Either[ExecutionError, Result] = steps match {
    case nextStep :: remainingSteps => {
      nextStep.execute(previousStepOutput).flatMap {
        stepResult => executeInternal(Some(stepResult), remainingSteps)
      }
    }
    case Nil => Right(previousStepOutput.head)
  }
}
