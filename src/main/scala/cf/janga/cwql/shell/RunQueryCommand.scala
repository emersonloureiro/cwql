package cf.janga.cwql.shell

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser.{Parser, ParserError}
import cf.janga.cwql.api.planner._

case class RunQuery(parser: Parser, planner: Planner, executor: Executor, query: String, console: Console) extends Command {

  override def run(): Unit = {
    val planning =
      for {
        parsedQuery <- parser.parse(query)
        queryPlan <- planner.plan(parsedQuery)
        resultSet <- executor.execute(queryPlan.steps)
      } yield resultSet

    planning match {
      case Right(resultSet) => {
        printResultSet(resultSet)
      }
      case Left(ParserError(line, column)) => {
        console.writeln(s"Parsing error: line $line, column $column")
        val emptySpaces = 1.until(column).foldLeft("")((output, _) => output + " ")
        console.writeln(query)
        console.writeln(s"$emptySpaces^")
      }
      case Left(plannerError: PlannerError) => {
        plannerError match {
          case StartTimeAfterEndTime => console.writeln("Start time after end time")
          case UnmatchedProjection(projection) => {
            val projectionName = projection.namespaceAlias.fold(projection.metric)(alias => s"$alias.${projection.metric}")
            console.writeln(s"No matching namespace for $projectionName")
          }
          case UnmatchedFilter(booleanExpression) => {
            val expressionName = booleanExpression.alias.fold(booleanExpression.left)(alias => s"$alias.${booleanExpression.left}")
            console.writeln(s"""Invalid condition at "$expressionName"""")
          }
        }
      }
      case Left(executionError: ExecutionError) => {
        executionError match {
          case CloudWatchClientError(message) => console.writeln(s"CloudWatch error: $message")
        }
      }
      case Left(exception: Throwable) => {
        console.writeln(s"Internal error: ${exception.getMessage}")
        exception.printStackTrace()
      }
      case Left(_) => {
        console.writeln(s"Unknown error, sorry :'(")
      }
    }
  }

  private def printResultSet(resultSet: ResultSet): Unit = resultSet.records match {
    case Nil => console.writeln("0 records returned")
    case _ => {
      val headers = List(List("Timestamp") ++ resultSet.records.head.data.map(entry => (entry._1, entry._2._2)).toSeq.sortWith((entry_1, entry_2) => entry_1._2 < entry_2._2).map(_._1))
      val data =
        resultSet.records.map {
          record => {
            List(record.timestamp) ++ record.data.values.toSeq.sortWith((entry_1, entry_2) => entry_1._2 < entry_2._2).map(_._1)
          }
        }
      console.writeln(format(headers ++ data))
    }
  }

  // Code from this point on taken from:
  // https://stackoverflow.com/questions/7539831/scala-draw-table-to-console
  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield (for (cell <- row) yield if (cell == null) 0 else cell.toString.length)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells = (for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%" + size + "s").format(item))
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: Seq[Int]): String = colSizes map {
    "-" * _
  } mkString("+", "+", "+")
}