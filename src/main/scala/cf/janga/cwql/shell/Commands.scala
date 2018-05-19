package cf.janga.cwql.shell

case class Exit(console: Console) extends Command {
  override def run(): Unit = {
    console.writeln("Exiting...")
    System.exit(0)
  }
}

case class MultilineCommand(console: Console) extends Command {
  override def run(): Unit = {
    console.writeln("Multi-line mode. After entering your query, enter \\e to execute it.")
  }
}

case class InvalidCommand(console: Console) extends Command {
  override def run(): Unit = {
    console.writeln("Invalid command")
  }
}

case class EmptyCommand(console: Console) extends Command {
  override def run(): Unit = ()
}