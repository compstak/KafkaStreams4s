package compstak.kafkastreams4s.tests

import cats.effect.{IO, Resource}
import cats.implicits._
import compstak.kafkastreams4s.testing.KafkaStreamsTestRunner
import compstak.kafkastreams4s.vulcan.{VulcanCodec, VulcanTable}
import org.apache.kafka.streams.StreamsBuilder

class VulcanCodecTest extends munit.FunSuite {
  test("Using `mapCodec` should serialize and deserialize using the VulcanCodec") {
    val input = Map("foo" -> 1, "bar" -> 2, "baz" -> 3, "qux" -> 2)

    val sb = new StreamsBuilder
    val table = VulcanTable[String, Int](sb, "origin")
    val result = table.mapCodec[VulcanCodec, VulcanCodec]

    Resource
      .liftF(result.to[IO]("out") >> IO(sb.build))
      .flatMap(topo => KafkaStreamsTestRunner.testDriverResource[IO](topo))
      .use(driver =>
        KafkaStreamsTestRunner.inputTestTable2[IO, VulcanCodec, VulcanCodec](driver, "origin", input.toList: _*) >>
          KafkaStreamsTestRunner
            .outputTestTable2[IO, VulcanCodec, String, VulcanCodec, Int](driver, "out")
            .map(outputData => assertEquals(outputData, input))
      )
      .unsafeToFuture
  }

}
