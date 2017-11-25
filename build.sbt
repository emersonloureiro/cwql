name := "sqlparser"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++=  Seq(
  "org.parboiled" %% "parboiled" % "2.1.4",
  "org.scalactic" %% "scalactic" % "3.0.4" % "test",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)
