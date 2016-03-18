name := "spark-iqmulus"

version := "0.1.0"

organization := "IGNF"

// scalaVersion := "2.11.7"
scalaVersion := "2.10.6"

spName := "IGNF/spark-iqmulus"

sparkVersion := "1.6.1"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" force(),
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile"
)

// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/IGNF/spark-iqmulus</url>
  <licenses>
    <license>
      <name>Apache License, Verision 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:IGNF/spark-iqmulus.git</url>
    <connection>scm:git:git@github.com:IGNF/spark-iqmulus.git</connection>
  </scm>
  <developers>
    <developer>
      <id>mbredif</id>
      <name>Mathieu Brédif</name>
      <url>http://mathieu.bredif.com</url>
    </developer>
  </developers>)

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  scalaBinaryVersion.value == "2.11"
}
