name := "spark-keywords"

version := "0.1.0"

scalaVersion := "2.11.8"

name := "spark-csv"

crossScalaVersions := Seq("2.10.5", "2.11.8")

sparkVersion := "2.0.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "compile" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "compile" force(),
  "org.apache.spark" %% "spark-mllib" % testSparkVersion.value % "compile" force()
)

spIgnoreProvided := true

spAppendScalaVersion := true

spIncludeMaven := true

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}