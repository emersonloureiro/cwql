package cf.janga.cwql.shell

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser.Parser
import cf.janga.cwql.api.planner.Planner

case class Exit(console: Console) extends Command {
  override def run(): Unit = {
    console.writeln("Exiting...")
    System.exit(1)
  }
}

case class RunQuery(parser: Parser, planner: Planner, executor: Executor, query: String, console: Console) extends Command {

  override def run(): Unit = {
    for {
      parsedQuery <- parser.parse(query)
      queryPlan <- planner.plan(parsedQuery)
      queryResult <- executor.execute(queryPlan.steps)
    } yield {
      println(queryResult)
    }
  }
}