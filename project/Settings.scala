import sbt._
import Keys._

object Settings {
  lazy val settings = Seq(
    organization := "com.lendap",
    version := "0.1." + sys.props.getOrElse("buildNumber", default="0-SNAPSHOT"),
    scalaVersion := "2.10.4",
    publishMavenStyle := true,
    publishArtifact in Test := false
  )

  lazy val testSettings = Seq(
    fork in Test := false,
    parallelExecution in Test := false
  )

  lazy val lshSettings = Seq(
    name := "lsh-scala"
  )
}
