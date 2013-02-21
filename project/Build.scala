import sbt._
import Keys._

object WekaBuild extends Build {
  
  lazy val weka = Project(id = "weka", base = file("."))
  
}