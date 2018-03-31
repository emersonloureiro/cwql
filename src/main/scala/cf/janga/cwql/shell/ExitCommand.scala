package cf.janga.cwql.shell

case class Exit(console: Console) extends Command {
  override def run(): Unit = {
    console.writeln("Exiting...")
    System.exit(0)
  }
}

