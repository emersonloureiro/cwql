package cf.janga.cwql.shell

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser.Parser
import cf.janga.cwql.api.planner.{Planner, ResultSet}

import scala.util.{Failure, Success, Try}

trait Command {
  def run(): Unit

  def console: Console
}

trait Console {
  def writeln(string: String): Unit

  def write(string: String): Unit
}

class CommandInterpreter(console: Console) {

  private val parser = new Parser()

  private val planner = new Planner()

  private val executor = new Executor()

  private var multilineMode = false

  private var multilineQuery = ""

  def beforeInput(): Unit = {
    if (!multilineMode) {
      console.write("> ")
    }
  }

  def handle(input: String): Unit = Option(input) match {
    case None => // no-op
    case Some(nonNullInput) if nonNullInput.trim().isEmpty => // no-op
    case Some(_) => parseInput(input).run()
  }

  private def parseInput(input: String): Command = {
    if (input.startsWith("\\")) {
      input match {
        case "\\q" => Exit(console)
        case "\\m" => {
          multilineMode = true
          MultilineCommand(console)
        }
        case "\\e" if multilineMode => {
          multilineMode = false
          RunQuery(parser, planner, executor, multilineQuery, console)
        }
        case _ => InvalidCommand(console)
      }
    } else {
      if (multilineMode) {
        multilineQuery += input
        EmptyCommand(console)
      } else {
        // Assuming it's a query
        RunQuery(parser, planner, executor, input, console)
      }
    }
  }
}
