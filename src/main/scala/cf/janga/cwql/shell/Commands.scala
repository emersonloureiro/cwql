package cf.janga.cwql.shell

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser.{Parser, ParserError}
import cf.janga.cwql.api.planner.{Planner, PlannerError, StartTimeAfterEndTime}

import scala.util.{Failure, Success}

case class Exit(console: Console) extends Command {
  override def run(): Unit = {
    console.writeln("Exiting...")
    System.exit(1)
  }
}

case class RunQuery(parser: Parser, planner: Planner, executor: Executor, query: String, console: Console) extends Command {

  override def run(): Unit = {
    val planning =
      for {
        parsedQuery <- parser.parse(query)
        queryPlan <- planner.plan(parsedQuery)
      } yield queryPlan

    planning match {
      case Left(ParserError(line, column)) => {
        console.writeln(s"Parsing error: line $line, column $column")
        val emptySpaces = 1.until(column).foldLeft("")((output, _) => output + " ")
        console.writeln(query)
        console.writeln(s"$emptySpaces^")
      }
      case Left(plannerError) => {
        plannerError match {
          case StartTimeAfterEndTime => console.writeln("Start time after end time")
        }
      }
      case Right(queryPlan) => {
        executor.execute(queryPlan.steps) match {
          case Success(resultSet) => console.writeln(s"$resultSet")
          case Failure(error) => console.writeln(s"$error")
        }
      }
    }
  }
}