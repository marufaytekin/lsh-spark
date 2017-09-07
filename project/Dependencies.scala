import sbt._

object Dependencies {

  lazy val version = new {
    val scalaTest = "2.2.2"
    val spark = "1.4.1"
  }

  lazy val library = new {
    val sparkCore ="org.apache.spark" %% "spark-core" % version.spark
    val sparkMLib ="org.apache.spark" %% "spark-mllib" % version.spark
    val test = "org.scalatest" %% "scalatest" % version.scalaTest % Test
  }

  val lshDependencies: Seq[ModuleID] = Seq(
    library.sparkCore,
    library.sparkMLib,
    library.test
  )

}
