import sbt._
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaVersion := "2.11.12",
      version      := "0.1"
    )),
    name := "sparknlp",
    resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
    libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "2.1.1", "databricks" % "spark-corenlp" % "0.2.0-s_2.11")
  )