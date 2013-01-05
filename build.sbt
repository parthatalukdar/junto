import AssemblyKeys._

name := "junto"

version := "1.5-SNAPSHOT"

organization := "None"

scalaVersion := "2.10.0"

crossPaths := false

retrieveManaged := true

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.10" % "2.1.0",
  "org.clapper" % "argot_2.10" % "1.0.0",
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "commons-logging" % "commons-logging" % "1.1.1",
  "log4j" % "log4j" % "1.2.17"
)

seq(assemblySettings: _*)

jarName in assembly := "junto-assembly.jar"

