name := "Fogbow"

version := "1.2.0"

organization := "None"

scalaVersion := "2.9.1"

crossPaths := false

retrieveManaged := true

libraryDependencies += "org.clapper" %% "argot" % "0.3.5"

seq(sbtassembly.Plugin.assemblySettings: _*)

jarName in Assembly := "fogbow-assembly.jar"

