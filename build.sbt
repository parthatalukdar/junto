import AssemblyKeys._

name := "Junto"

version := "1.2.1"

organization := "None"

scalaVersion := "2.9.1"

crossPaths := false

retrieveManaged := true

libraryDependencies += "org.clapper" %% "argot" % "0.3.5"

libraryDependencies += "net.sf.trove4j" % "trove4j" % "3.0.1"

seq(assemblySettings: _*)

jarName in assembly := "junto-assembly.jar"

