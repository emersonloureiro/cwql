package cf.janga.cwql.executor

import cf.janga.cwql.planner.{ResultSet, Step}

import scala.util.{Success, Try}

class Executor {

  def execute(steps: Seq[Step]): Try[ResultSet] = {
    executeInternal(None, steps)
  }

  private def executeInternal(previousStepOutput: Option[ResultSet], steps: Seq[Step]): Try[ResultSet] = steps match {
    case nextStep :: remainingSteps => {
      nextStep.execute(previousStepOutput).flatMap {
        stepResult => executeInternal(Some(stepResult), remainingSteps)
      }
    }
    case Nil => Success(previousStepOutput.head)
  }
}
