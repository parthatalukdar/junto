name := "junto"

version := "1.5-SNAPSHOT"

organization := "None"

scalaVersion := "2.10.6"

crossPaths := false

retrieveManaged := true

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.1.0",
  "org.clapper" %% "argot" % "1.0.0",
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "com.typesafe" %% "scalalogging-log4j" % "1.0.1")


jarName in assembly := "junto-assembly.jar"

