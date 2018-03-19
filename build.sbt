import org.ensime.EnsimeCoursierKeys._

name := "cwql"

version := "1.0"

scalaVersion in ThisBuild := "2.12.2"

ensimeServerVersion in ThisBuild := "2.0.0-M1"

libraryDependencies ++=  Seq(
  "org.parboiled" %% "parboiled" % "2.1.4",
  "org.scalactic" %% "scalactic" % "3.0.4" % "test",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.amazonaws" % "aws-java-sdk-cloudwatch" % "1.11.292",
  "joda-time" % "joda-time" % "2.9.9",
  "com.github.scopt" %% "scopt" % "3.7.0"
)
