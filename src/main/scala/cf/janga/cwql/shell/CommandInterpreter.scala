package cf.janga.cwql.shell

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser.Parser
import cf.janga.cwql.api.planner.{Planner, ResultSet}

import scala.util.{Failure, Success, Try}

sealed trait Signal

case class QueryResult(resultSet: Option[ResultSet], error: Option[String]) extends Signal

case class InvalidInput(details: Option[String]) extends Signal

case object EmptyInput extends Signal

trait Command {
  def run(): Unit

  def console: Console
}

trait Console {
  def writeln(string: String): Unit
}

class CommandInterpreter(console: Console) {

  private val parser = new Parser()

  private val planner = new Planner()

  private val executor = new Executor()

  def handle(input: String): Unit = Option(input) match {
    case None => // no-op
    case Some(nonNullInput) if nonNullInput.trim().isEmpty => // no-op
    case Some(_) => parseInput(input) match {
      case Success(command) => command.run()
      case Failure(error) => console.writeln(error.getMessage)
    }
  }

  private def parseInput(input: String): Try[Command] = {
    if (input.startsWith("\\")) {
      input match {
        case "\\q" => Success(Exit(console))
        case _ => Failure(new RuntimeException("unrecognized command"))
      }
    } else {
      // Assuming it's a query
      Success(RunQuery(parser, planner, executor, input, console))
    }
  }
}
