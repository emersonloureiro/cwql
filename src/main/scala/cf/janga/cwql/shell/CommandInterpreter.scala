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
    case Some(_) => {
      for {
        command <- parseInput(input)
      } {
        command.run()
      }
    }
  }

  private def parseInput(input: String): Option[Command] = {
    if (input.startsWith("\\")) {
      input match {
        case "\\q" => Some(Exit(console))
        case _ => {
          console.writeln("Invalid command")
          None
        }
      }
    } else {
      // Assuming it's a query
      Some(RunQuery(parser, planner, executor, input, console))
    }
  }
}
