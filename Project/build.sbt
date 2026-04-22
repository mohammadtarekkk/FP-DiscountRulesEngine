ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
lazy val root = (project in file("."))
  .settings(
    name := "DiscountRulesEngine"
  )

libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.4.0",
    "org.apache.spark" %% "spark-sql" % "3.4.0",
)
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"

testFrameworks += new TestFramework("munit.Framework")

