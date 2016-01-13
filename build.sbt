import Common._
import sbt.Keys._

organization := "io.ddf"

name := "ddf"

version := ddfVersion

retrieveManaged := true // Do create a lib_managed, so we have one place for all the dependency jars to copy to slaves, if needed

scalaVersion := theScalaVersion

scalacOptions := Seq("-unchecked", "-optimize", "-deprecation")

// Fork new JVMs for tests and set Java options for those
fork in Test := true

parallelExecution in ThisBuild := false

javaOptions in Test ++= Seq("-Xmx2g")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

conflictManager := ConflictManager.strict

commonSettings

lazy val jdbcAssemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.endsWith("eclipsef.sf") => MergeStrategy.discard
      case m if m.toLowerCase.endsWith("eclipsef.rsa") => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case "application.conf"                            => MergeStrategy.concat
    case _ => MergeStrategy.first
  },
  test in assembly := {} 
)

lazy val root = project.in(file(".")).aggregate(jdbc, jdbcExamples,jdbcTest,postgres,aws)

val com_adatao_unmanaged = Seq(
  "com.adatao.unmanaged.net.rforge" % "REngine" % "2.1.1.compiled",
  "com.adatao.unmanaged.net.rforge" % "Rserve" % "1.8.2.compiled"
)

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

lazy val jdbc = project.in(file("jdbc")).settings(commonSettings: _*).settings(jdbcAssemblySettings:_*).settings(
  name := "ddf-jdbc",
  pomExtra := submodulePom,
  libraryDependencies ++= Seq(
    "com.zaxxer" % "HikariCP-java6" % "2.3.9",
    "org.scalikejdbc" %% "scalikejdbc" % "2.2.7",
    "com.univocity" % "univocity-parsers" % "1.5.5",
    "com.clearspring.analytics" % "stream" % "2.7.0" exclude("asm", "asm")
  )
)

lazy val jdbcExamples = project.in(file("jdbc-examples")).dependsOn(jdbc).settings(commonSettings: _*).settings(jdbcAssemblySettings:_*).settings(
  name := "ddf-jdbc-examples",
  pomExtra := submodulePom
)

lazy val jdbcTest = project.in(file("jdbc-test")).dependsOn(jdbc).settings(commonSettings: _*).settings(jdbcAssemblySettings:_*).settings(
  name := "ddf-jdbc-test",
  pomExtra := submodulePom,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.0-M7",
    "com.h2database" % "h2" % "1.4.187" % "test"
  )
)

lazy val postgres = project.in(file("postgres")).dependsOn(jdbc,jdbcTest % "test->test").settings(commonSettings: _*).settings(jdbcAssemblySettings:_*).settings(
  name := "ddf-jdbc-postgres",
  pomExtra := submodulePom,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.0-M7" % "test",
    "postgresql" % "postgresql" % "9.1-901-1.jdbc4"
  )
)

lazy val aws = project.in(file("aws")).dependsOn(jdbc,postgres,jdbcTest % "test->test").settings(commonSettings: _*).settings(jdbcAssemblySettings:_*).settings(
  name := "ddf-jdbc-aws",
  pomExtra := submodulePom,
  libraryDependencies ++= Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.10.8"
  )
)

resolvers ++= Seq("Adatao Mvnrepos Snapshots" at "https://raw.github.com/adatao/mvnrepos/master/snapshots",
  "Adatao Mvnrepos Releases" at "https://raw.github.com/adatao/mvnrepos/master/releases")

publishMavenStyle := true
