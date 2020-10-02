// *****************************************************************************
// Projects
// *****************************************************************************

lazy val adminclient =
  project
    .in(file("."))
    .enablePlugins(AutomateHeaderPlugin)
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= Seq(
        library.clients,
        library.kafka,
        library.scalatest % Test,
        library.munit           % Test,
        library.munitScalaCheck % Test,
      ),
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val munit = "0.7.11"
      val kafka = "2.6.0"
      val scalatest = "3.2.0"
    }
    val clients = "org.apache.kafka" % "kafka-clients" % Version.kafka
    val kafka = "org.apache.kafka" %% "kafka" % Version.kafka
    val munit           = "org.scalameta" %% "munit"            % Version.munit
    val munitScalaCheck = "org.scalameta" %% "munit-scalacheck" % Version.munit
    val scalatest = "org.scalatest" %% "scalatest" % Version.scalatest
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val commonSettings =
  Seq(
    scalaVersion := "2.13.3",
    organization := "default",
    organizationName := "ksilin",
    startYear := Some(2020),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-encoding", "UTF-8",
      "-Ywarn-unused:imports",
    ),
   // testFrameworks += new TestFramework("munit.Framework"),
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
      scalafmtOnCompile := true,
)
