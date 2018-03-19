package cf.janga.cwql.shell

import scopt.OptionParser
import scala.annotation.tailrec

case class ShellArguments(verbose: Boolean = false, profile: Option[String] = None)

object Shell {

  def start(startupOptions: Array[String]): Unit = {
    val argumentParser = new OptionParser[ShellArguments]("cqwl") {
      head("cwql")

      opt[Unit]('v', "verbose").action((_, shellArguments) => shellArguments.copy(verbose = true)).text("Verbose mode")
    }

    argumentParser.parse(Option(startupOptions).getOrElse(Array.empty[String]).toSeq, ShellArguments()) match {
      case Some(shellArguments) => {
        println("cwql started!")
        readInput(new InputReader(), new CommandInterpreter())
      }
      case None => // no-op error will be printed out by scopt
    }
  }

  @tailrec
  private def readInput(reader: InputReader, commandInterpreter: CommandInterpreter): Unit = {
    print("> ")
    val input = reader.readInput()
    commandInterpreter.handle(input) match {
      case Exit => println("Exiting...")
      case QueryResult(Some(resultSet), _) => {
        println(resultSet)
        readInput(reader, commandInterpreter)
      }
      case QueryResult(_, Some(error)) => {
        println(s"Error running query: $error")
        readInput(reader, commandInterpreter)
      }
      case EmptyInput => readInput(reader, commandInterpreter)
      case InvalidInput(detailsOption) => {
        val error = detailsOption.fold("Invalid input")(details => s"Invalid input: $details")
        println(error)
        readInput(reader, commandInterpreter)
      }
    }
  }
}
