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
import cats.effect.Resource
import cats.effect.unsafe.implicits.global

class STableOpsTest extends munit.FunSuite {

  test("STable scanWith should work as expected") {
    val input = List("foo" -> 1, "bar" -> 2, "baz" -> 3, "qux" -> 2)

    KafkaStreamsTestRunner
      .run[IO, CirceCodec, String, Int, Int, String](_.scanWith((k, v) => (v, k))(_ + _), input: _*)
      .map(out => assertEquals(Map(1 -> "foo", 2 -> "barqux", 3 -> "baz"), out))
      .unsafeToFuture()
  }

  test("STable join should work as expected") {
    val inputA = List(1 -> "foo", 2 -> "bar")
    val inputB = List("foo" -> 1, "bar" -> 2)

    val out = "out"

    val sb = new StreamsBuilder
    val tableA = CirceTable[Int, String](sb, "a")
    val tableB = CirceTable[String, Int](sb, "b")

    val result = tableA.join(tableB)(identity)((s, i) => s * i)

    Resource
      .eval(result.to[IO](out) >> IO(sb.build))
      .flatMap(topo => KafkaStreamsTestRunner.testDriverResource[IO](topo))
      .use(driver =>
        KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "a", inputA: _*) >>
          KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "b", inputB: _*) >>
          KafkaStreamsTestRunner
            .outputTestTable[IO, CirceCodec, Int, String](driver, out)
            .map(res => assertEquals(Map(1 -> "foo", 2 -> "barbar"), res))
      )
      .unsafeToFuture

  }

  test("STable joinOption should work as expected") {
    val inputA = List(1 -> "1", 2 -> "two", 3 -> "3")
    val inputB = List(1 -> "foo", 2 -> "bar", 3 -> "baz").map { case (n, s) => (n: Integer, s) }

    val out = "out"

    val sb = new StreamsBuilder
    val tableA = CirceTable[Int, String](sb, "a")
    val tableB = CirceTable[Integer, String](sb, "b")

    val result = tableA.joinOption(tableB)(s => Try(s.toInt: Integer).toOption)((a, b) => s"$a:$b")

    Resource
      .eval(result.to[IO](out) >> IO(sb.build))
      .flatMap(topo => KafkaStreamsTestRunner.testDriverResource[IO](topo))
      .use(driver =>
        KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "a", inputA: _*) >>
          KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "b", inputB: _*) >>
          KafkaStreamsTestRunner
            .outputTestTable[IO, CirceCodec, Int, String](driver, out)
            .map(res => assertEquals(Map(1 -> "1:foo", 3 -> "3:baz"), res))
      )
      .unsafeToFuture

  }

  test("STable leftJoin should work as expected") {
    val inputA = List(1 -> "foo", 2 -> "bar")
    val inputB = List("bar" -> 2)

    val out = "out"

    val sb = new StreamsBuilder
    val tableA = CirceTable[Int, String](sb, "a")
    val tableB = CirceTable[String, Int](sb, "b")

    val result = tableA.leftJoin(tableB)(identity)((s, i) => s * i.getOrElse(1))

    Resource
      .eval(result.to[IO](out) >> IO(sb.build))
      .flatMap(topo => KafkaStreamsTestRunner.testDriverResource[IO](topo))
      .use(driver =>
        KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "a", inputA: _*) >>
          KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "b", inputB: _*) >>
          KafkaStreamsTestRunner
            .outputTestTable[IO, CirceCodec, Int, String](driver, out)
            .map(res => assertEquals(Map(1 -> "foo", 2 -> "barbar"), res))
      )
      .unsafeToFuture

  }

  test("STable keyJoin should work as expected") {
    val inputA = List(1 -> "foo", 2 -> "bar")
    val inputB = List(1 -> "baz", 3 -> "qux")

    val out = "out"

    val sb = new StreamsBuilder
    val tableA = CirceTable[Int, String](sb, "a")
    val tableB = CirceTable[Int, String](sb, "b")

    val result = tableA.keyJoin(tableB)((a, b) => s"$a:$b")

    Resource
      .eval(result.to[IO](out) >> IO(sb.build))
      .flatMap(topo => KafkaStreamsTestRunner.testDriverResource[IO](topo))
      .use(driver =>
        KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "a", inputA: _*) >>
          KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "b", inputB: _*) >>
          KafkaStreamsTestRunner
            .outputTestTable[IO, CirceCodec, Int, String](driver, out)
            .map(res => assertEquals(Map(1 -> "foo:baz"), res))
      )
      .unsafeToFuture

  }

  test("STable keyOuterJoin should work as expected") {
    val inputA = List(1 -> "foo", 2 -> "bar")
    val inputB = List(1 -> "baz", 3 -> "qux")

    val out = "out"

    val sb = new StreamsBuilder
    val tableA = CirceTable[Int, String](sb, "a")
    val tableB = CirceTable[Int, String](sb, "b")

    val result = tableA.keyOuterJoin(tableB)(ior => ior.merge)

    Resource
      .eval(result.to[IO](out) >> IO(sb.build))
      .flatMap(topo => KafkaStreamsTestRunner.testDriverResource[IO](topo))
      .use(driver =>
        KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "a", inputA: _*) >>
          KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "b", inputB: _*) >>
          KafkaStreamsTestRunner
            .outputTestTable[IO, CirceCodec, Int, String](driver, out)
            .map(res => assertEquals(Map(1 -> "foobaz", 2 -> "bar", 3 -> "qux"), res))
      )
      .unsafeToFuture

  }

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

  test("STable collect should work as expected") {
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
