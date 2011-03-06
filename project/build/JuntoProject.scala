import sbt._

class JuntoProject (info: ProjectInfo) extends DefaultProject(info) {
  override def disableCrossPaths = true 
  val argot = "org.clapper" %% "argot" % "0.2"
}


