package cf.janga.sqlparser

case class Query(projection: Projection, from: From)

case class Projection(values: Seq[String])

case class From(values: Seq[String])
