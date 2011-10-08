import AssemblyKeys._

name := "Junto"

version := "1.2.0"

organization := "None"

scalaVersion := "2.9.1"

crossPaths := false

retrieveManaged := true

libraryDependencies += "org.clapper" %% "argot" % "0.3.5"

//seq(sbtassembly.Plugin.assemblySettings: _*)

seq(assemblySettings: _*)

jarName in assembly := "junto-assembly.jar"

