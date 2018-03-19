package cf.janga.cwql.shell

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser.Parser
import cf.janga.cwql.api.planner.{Planner, ResultSet}

import scala.util.{Failure, Success, Try}

sealed trait Signal
case class QueryResult(resultSet: Option[ResultSet], error: Option[String]) extends Signal
case class InvalidInput(details: Option[String]) extends Signal
case object EmptyInput extends Signal

sealed trait Command
case object Exit extends Signal with Command
case class Query(query: String) extends Command

class CommandInterpreter {

  private val parser = new Parser()

  private val planner = new Planner()

  private val executor = new Executor()

  def handle(input: String): Signal = input match {
    case null => InvalidInput(None)
    case nonNullInput if nonNullInput.isEmpty => EmptyInput
    case _ => parseInput(input) match {
      case Success(Exit) => Exit
      case Success(Query(query)) => {
        runQuery(query) match {
          case Success(resultSet) => QueryResult(Some(resultSet), None)
          case Failure(error) => QueryResult(None, Some(error.getMessage))
        }
      }
      case Failure(error) => InvalidInput(Some(error.getMessage))
    }
  }

  private def parseInput(input: String): Try[Command] = {
    if (input.startsWith("\\")) {
      input match {
        case "\\q" => Success(Exit)
        case _ => Failure(new RuntimeException("unrecognized command"))
      }
    } else {
      // Assuming it's a query
      Success(Query(input))
    }
  }

  private def runQuery(query: String): Try[ResultSet] = {
    for {
      parsedQuery <- parser.parse(query)
      queryPlan <- planner.plan(parsedQuery)
      queryResult <- executor.execute(queryPlan.steps)
    } yield {
      queryResult
    }
  }
}
