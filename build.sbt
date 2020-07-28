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

enablePlugins(DockerComposePlugin)

val Avro4sVersion = "3.1.1"
val CatsEffectVersion = "2.1.3"
val CirceVersion = "0.13.0"
val CirceDebeziumVersion = "0.12.0"
val DoobieVersion = "0.8.8"
val FS2KafkaVersion = "1.0.0"
val Http4sVersion = "0.21.6"
val KafkaVersion = "2.5.0"
val KafkaConnectHttp4sVersion = "0.5.0"
val MunitVersion = "0.7.9"
val ShapelessVersion = "2.3.3"

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
addCommandAlias("validate", ";fmtCheck; test; it:compile")

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
    name := "kafka-streams4s-circe",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % KafkaVersion,
      "io.circe" %% "circe-parser" % CirceVersion
    )
  )
  .dependsOn(core)

lazy val avro4s = (project in file("avro"))
  .settings(commonSettings)
  .settings(
    name := "kafka-streams4s-avro4s",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % KafkaVersion,
      "com.sksamuel.avro4s" %% "avro4s-kafka" % Avro4sVersion
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

lazy val shapeless = (project in file("shapeless"))
  .settings(commonSettings)
  .settings(
    name := "kafka-streams4s-shapeless",
    libraryDependencies += "com.chuusai" %% "shapeless" % "2.3.3"
  )
  .dependsOn(debezium)

lazy val testing = (project in file("testing"))
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(
    name := "kafka-streams4s-tests",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams-test-utils" % KafkaVersion,
      "com.github.fd4s" %% "fs2-kafka" % FS2KafkaVersion,
      "org.scalameta" %% "munit" % MunitVersion % Test,
      "com.compstak" %% "kafka-connect-migrate" % KafkaConnectHttp4sVersion % IntegrationTest,
      "io.circe" %% "circe-literal" % CirceVersion % IntegrationTest,
      "org.http4s" %% "http4s-async-http-client" % Http4sVersion % IntegrationTest,
      "org.tpolecat" %% "doobie-postgres" % DoobieVersion % IntegrationTest
    ),
    Defaults.itSettings,
    inConfig(IntegrationTest)(ScalafmtPlugin.scalafmtConfigSettings),
    testFrameworks += new TestFramework("munit.Framework")
  )
  .dependsOn(core, circe, avro4s, debezium, shapeless)

lazy val kafkaStreams4s = (project in file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(name := "kafka-streams4s")
  .dependsOn(core, circe, debezium, avro4s, shapeless, testing)
  .aggregate(core, circe, debezium, avro4s, shapeless, testing)
