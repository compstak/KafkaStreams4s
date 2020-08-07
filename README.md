# KafkaStreams4s

[![CircleCI](https://circleci.com/gh/compstak/KafkaStreams4s.svg?style=svg)](https://circleci.com/gh/compstak/KafkaStreams4s) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.compstak/kafka-streams4s-core_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.compstak/kafka-streams4s-core_2.13) 

KafkaStreams4s is a library for writing Kafka Streams programs using [cats-effect](https://github.com/typelevel/cats-effect).
To use KafkaStreams4s in an existing SBT project with Scala 2.12 or a later version, add the following dependencies to your build.sbt depending on your needs:

```scala
libraryDependencies ++= Seq(
  "com.compstak" %% "kafka-streams4s-core" % "<version>",
  "com.compstak" %% "kafka-streams4s-circe" % "<version>",
  "com.compstak" %% "kafka-streams4s-avro4s" % "<version>",
  "com.compstak" %% "kafka-streams4s-debezium" % "<version>",
  "com.compstak" %% "kafka-streams4s-shapeless" % "<version>",
  "com.compstak" %% "kafka-streams4s-testing" % "<version>" % Test
)
```

To learn more head to the [documentation page](docs/out/Intro.md).

