import AssemblyKeys._

name := "junto"

version := "1.5-SNAPSHOT"

organization := "None"

scalaVersion := "2.10.0"

crossPaths := false

retrieveManaged := true

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.10" % "2.1.0",
  "org.clapper" % "argot_2.10" % "1.0.0",
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "com.typesafe" % "scalalogging-log4j_2.10" % "1.0.1")

seq(assemblySettings: _*)

jarName in assembly := "junto-assembly.jar"

