package cf.janga.sqlparser

import org.parboiled2._

class SqlParser(val input: ParserInput) extends Parser {

  def Sql = rule {
    SelectRule ~ WSRule ~ ignoreCase("from") ~ WSRule ~ FromRule ~ WSRule ~ EOI ~> Query
  }

  def SelectRule = rule {
    WSRule ~ ignoreCase("select") ~ WSRule ~ IdentifiersRule ~> Projection
  }

  def IdentifierRule = rule {
    capture(oneOrMore(anyOf("abcdefghijklmnopqrstuvwyxz0123456789_")))
  }

  def IdentifiersRule = rule {
    oneOrMore(IdentifierRule).separatedBy(str(",") ~ WSRule)
  }

  def FromRule = rule {
    IdentifiersRule ~> From
  }

  def WSRule = rule {
    zeroOrMore(anyOf(" \t \n"))
  }
}

object Main extends App {

  val result = new SqlParser("select status, time from requests where ").Sql.run()
  println(result)
}