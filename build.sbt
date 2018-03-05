import org.ensime.EnsimeCoursierKeys._

name := "cwql"

version := "1.0"

scalaVersion in ThisBuild := "2.12.2"

ensimeServerVersion in ThisBuild := "2.0.0-M1"

libraryDependencies ++=  Seq(
  "org.parboiled" %% "parboiled" % "2.1.4",
  "org.scalactic" %% "scalactic" % "3.0.4" % "test",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)
