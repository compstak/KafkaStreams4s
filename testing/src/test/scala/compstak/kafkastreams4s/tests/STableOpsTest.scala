package compstak.kafkastreams4s

import org.apache.kafka.streams.StreamsBuilder
import compstak.kafkastreams4s.circe.CirceTable
import cats.effect.IO
import scala.concurrent.ExecutionContext
import cats.implicits._
import compstak.kafkastreams4s.circe.CirceCodec
import scala.concurrent.duration._
import compstak.kafkastreams4s.testing.KafkaStreamsTestRunner
import scala.util.Try

class STableOpsTest extends munit.FunSuite {

  test("STable keyBy should work as expected") {
    val input = List("foo" -> 1, "bar" -> 2, "baz" -> 3, "qux" -> 4)

    KafkaStreamsTestRunner
      .run[IO, CirceCodec, String, Int, Int, Int](_.keyBy(identity), input: _*)
      .map(out => assertEquals(Map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4), out))
      .unsafeToFuture()
  }

  test("STable reKey should work as expected") {
    val input = List("foo" -> 1, "barr" -> 2, "bazzz" -> 3, "quxxxx" -> 4)

    KafkaStreamsTestRunner
      .run[IO, CirceCodec, String, Int, Int, Int](_.reKey(_.length), input: _*)
      .map(out => assertEquals(Map(3 -> 1, 4 -> 2, 5 -> 3, 6 -> 4), out))
      .unsafeToFuture()
  }

  test("STable transform should work as expected") {
    val input = List("foo" -> 1, "barr" -> 2, "bazzz" -> 3, "quxxxx" -> 4)

    KafkaStreamsTestRunner
      .run[IO, CirceCodec, String, Int, Int, Int](_.transform((s, i) => (i + 3, s.length)), input: _*)
      .map(out => assertEquals(Map(4 -> 3, 5 -> 4, 6 -> 5, 7 -> 6), out))
      .unsafeToFuture()
  }

  test("STable scan should work as expected") {
    val input = List("foo", "barr", "bazzz", "quxxxx")

    KafkaStreamsTestRunner
      .runList[IO, CirceCodec, String, Int](_.scan(0)((acc, cur) => acc + cur.length), input: _*)
      .map(out => assertEquals(Set(3, 7, 12, 18), out.toSet))
      .unsafeToFuture()
  }

  test("STable scan1 should work as expected") {
    val input = List(1, 2, 3, 4)

    KafkaStreamsTestRunner
      .runList[IO, CirceCodec, Int, Int](_.scan1(_ + _), input: _*)
      .map(out => assertEquals(Set(1, 3, 6, 10), out.toSet))
      .unsafeToFuture()
  }

  test("STable map should work as expected") {
    val input = List(1, 2, 3, 4)

    KafkaStreamsTestRunner
      .runList[IO, CirceCodec, Int, Int](_.map(_ + 1), input: _*)
      .map(out => assertEquals(out, List(2, 3, 4, 5)))
      .unsafeToFuture()
  }

  test("STable mapWithKey should work as expected") {
    val input = List(1, 2, 3, 4)

    KafkaStreamsTestRunner
      .runList[IO, CirceCodec, Int, Int](_.mapWithKey((k, v) => v + 1), input: _*)
      .map(out => assertEquals(out, List(2, 3, 4, 5)))
      .unsafeToFuture()
  }

  test("STable mapFilter should work as expected") {
    val input = List("1", "2", "three", "3")

    KafkaStreamsTestRunner
      .runList[IO, CirceCodec, String, Int](_.mapFilter(s => Try(s.toInt).toOption), input: _*)
      .map(out => assertEquals(out, List(1, 2, 3)))
      .unsafeToFuture()
  }

  test("STable mapFilterWithKey should work as expected") {
    val input = List(1 -> "1", 2 -> "2", 3 -> "three", 4 -> "3")

    KafkaStreamsTestRunner
      .run[IO, CirceCodec, Int, String, Int, Int](
        _.mapFilterWithKey((k, v) => Try(v.toInt).toOption.filter(_ == k)),
        input: _*
      )
      .map(out => assertEquals(out, Map(1 -> 1, 2 -> 2)))
      .unsafeToFuture()
  }

  test("STable flattenOption should work as expected") {
    val input = List(Option("foo"), None, Option("bar"))

    KafkaStreamsTestRunner
      .runList[IO, CirceCodec, Option[String], String](_.collect { case Some(s) => s }, input: _*)
      .map(out => assertEquals(out, List("foo", "bar")))
      .unsafeToFuture()
  }

  test("STable flattenOption should work as expected") {
    val input = List(Option("foo"), None, Option("bar"))

    KafkaStreamsTestRunner
      .runList[IO, CirceCodec, Option[String], String](_.flattenOption, input: _*)
      .map(out => assertEquals(out, List("foo", "bar")))
      .unsafeToFuture()
  }

}
