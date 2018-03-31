package cf.janga.cwql.shell

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser.{Parser, ParserError}
import cf.janga.cwql.api.planner.{Planner, PlannerError, ResultSet, StartTimeAfterEndTime}

case class RunQuery(parser: Parser, planner: Planner, executor: Executor, query: String, console: Console) extends Command {

  override def run(): Unit = {
    val planning =
      for {
        parsedQuery <- parser.parse(query)
        queryPlan <- planner.plan(parsedQuery)
        resultSet <- executor.execute(queryPlan.steps).toEither
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
        }
      }
      case Left(exception: Throwable) => {
        console.writeln("Internal error")
        exception.printStackTrace()
      }
    }
  }

  private def printResultSet(resultSet: ResultSet): Unit = resultSet.records match {
    case Nil => console.writeln("0 records returned")
    case _ => {
      val headers = List(List("Timestamp") ++ resultSet.records.head.data.keys.toSeq)
      val data =
        resultSet.records.map {
          record => {
            List(record.timestamp) ++ record.data.values.toSeq
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