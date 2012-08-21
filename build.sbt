import AssemblyKeys._

name := "junto"

version := "1.4-SNAPSHOT"

organization := "None"

scalaVersion := "2.9.2"

crossPaths := false

retrieveManaged := true

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor" % "2.0.3",
  "org.clapper" %% "argot" % "0.4",
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "commons-logging" % "commons-logging" % "1.1.1",
  "log4j" % "log4j" % "1.2.16"
)

seq(assemblySettings: _*)

jarName in assembly := "junto-assembly.jar"

