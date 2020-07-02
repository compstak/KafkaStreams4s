lazy val scala212 = "2.12.10"
lazy val scala213 = "2.13.2"
lazy val supportedScalaVersions = List(scala213, scala212)

inThisBuild(
  List(
    scalaVersion := scala213,
    organization := "com.compstak",
    homepage := Some(url("https://github.com/compstak/circe-geojson")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "LukaJCB",
        "Luka Jacobowitz",
        "luka.jacobowitz@gmail.com",
        url("https://github.com/LukaJCB")
      ),
      Developer(
        "paul-snively",
        "Paul Snively",
        "psnively@mac.com",
        url("https://github.com/paul-snively")
      )
    )
  )
)

val CatsEffectVersion = "2.1.3"
val CirceVersion = "0.13.0"
val CirceDebeziumVersion = "0.8.0"
val KafkaVersion = "2.5.0"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-language:higherKinds",
  "-language:postfixOps",
  "-feature",
  "-Xfatal-warnings"
)

addCommandAlias("fmtAll", ";scalafmt; test:scalafmt; scalafmtSbt")
addCommandAlias("fmtCheck", ";scalafmtCheck; test:scalafmtCheck; scalafmtSbtCheck")
addCommandAlias("validate", ";fmtCheck; test")

lazy val commonSettings = Seq(
  crossScalaVersions := supportedScalaVersions,
  addCompilerPlugin(("org.typelevel" %% "kind-projector" % "0.11.0").cross(CrossVersion.full)),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  scalafmtOnCompile := true
)

lazy val noPublishSettings = Seq(
  skip in publish := true
)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "kafka-streams4s-core",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % KafkaVersion,
      "org.typelevel" %% "cats-effect" % CatsEffectVersion
    )
  )

lazy val circe = (project in file("circe"))
  .settings(commonSettings)
  .settings(
    name := "kafka-streams4s-core",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % KafkaVersion,
      "io.circe" %% "circe-parser" % CirceVersion
    )
  )
  .dependsOn(core)

lazy val debezium = (project in file("debezium"))
  .settings(commonSettings)
  .settings(
    name := "kafka-streams4s-debezium",
    libraryDependencies ++= Seq(
      "com.compstak" %% "circe-debezium" % CirceDebeziumVersion,
      "io.circe" %% "circe-parser" % CirceVersion
    )
  )
  .dependsOn(circe)

lazy val tests = (project in file("tests"))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(name := "kafka-streams4s-tests")
  .dependsOn(core, circe, debezium)

lazy val kafkaStreams4s = (project in file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(name := "kafka-streams4s")
  .dependsOn(core, circe, debezium, tests)
  .aggregate(core, circe, debezium, tests)
