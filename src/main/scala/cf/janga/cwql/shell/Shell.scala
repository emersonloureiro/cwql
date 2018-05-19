package cf.janga.cwql.shell

import scopt.OptionParser
import scala.annotation.tailrec

case class ShellArguments(verbose: Boolean = false, profile: Option[String] = None)

object StdoutConsole extends Console {
  override def writeln(string: String): Unit = {
    println(string)
  }

  override def write(string: String): Unit = {
    print(string)
  }
}

object Shell extends App {

  start(args)

  def start(startupOptions: Array[String]): Unit = {
    val argumentParser = new OptionParser[ShellArguments]("cqwl") {
      head("cwql")

      opt[Unit]('v', "verbose").action((_, shellArguments) => shellArguments.copy(verbose = true)).text("Verbose mode")
    }

    argumentParser.parse(Option(startupOptions).getOrElse(Array.empty[String]).toSeq, ShellArguments()) match {
      case Some(shellArguments) => {
        println("cwql started!")
        readInput(new InputReader(), new CommandInterpreter(StdoutConsole))
      }
      case None => // no-op error will be printed out by scopt
    }
  }

  @tailrec
  private def readInput(reader: InputReader, commandInterpreter: CommandInterpreter): Unit = {
    commandInterpreter.beforeInput()
    val input = reader.readInput()
    commandInterpreter.handle(input)
    readInput(reader, commandInterpreter)
  }
}
