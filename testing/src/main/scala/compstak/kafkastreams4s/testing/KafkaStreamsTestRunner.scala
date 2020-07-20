package compstak.kafkastreams4s.testing

import org.apache.kafka.streams.Topology
import fs2.kafka._
import cats.effect.ConcurrentEffect
import cats.effect.ContextShift
import scala.util.Random
import cats.effect.Sync
import cats.implicits._
import cats.effect.Timer
import cats.effect.implicits._
import java.time.Duration
import java.{util => ju}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.StreamsBuilder
import scala.concurrent.duration._
import org.apache.kafka.streams.TopologyTestDriver
import cats.effect.Resource
import scala.collection.JavaConverters._
import compstak.kafkastreams4s._
import org.apache.kafka.streams.TestOutputTopic

class KafkaStreamsTestRunner[F[_]: Sync, C[_]: Codec, KA: C, A: C, KB: C, B: C](
  f: STable[C, KA, A] => STable[C, KB, B]
) {

  private def randomString: F[String] = Sync[F].delay(Random.alphanumeric.take(32).mkString)

  def testDriverResource(topo: Topology): Resource[F, TopologyTestDriver] =
    Resource.make(Sync[F].delay(new TopologyTestDriver(topo, props)))(d => Sync[F].delay(d.close))

  def runTestTopologyMap(
    topicIn: String,
    topicOut: String,
    driver: TopologyTestDriver,
    input: Seq[(KA, A)]
  ): F[Map[KB, B]] =
    runTestTopology(topicIn, topicOut, driver, input).flatMap(out =>
      Sync[F].delay(
        out.readKeyValuesToMap.asScala.toMap
      )
    )

  private def runTestTopology(
    topicIn: String,
    topicOut: String,
    driver: TopologyTestDriver,
    input: Seq[(KA, A)]
  ): F[TestOutputTopic[KB, B]] = {
    val in = driver.createInputTopic(topicIn, Codec[C].serde[KA].serializer, Codec[C].serde[A].serializer)
    val out = driver.createOutputTopic(topicOut, Codec[C].serde[KB].deserializer, Codec[C].serde[B].deserializer)
    input.toList.traverse_ { case (k, a) => Sync[F].delay(in.pipeInput(k, a)) }.as(out)
  }

  def runTestTopologyValues(
    topicIn: String,
    topicOut: String,
    driver: TopologyTestDriver,
    input: Seq[(KA, A)]
  ): F[List[B]] =
    runTestTopology(topicIn, topicOut, driver, input).flatMap(out =>
      Sync[F].delay(
        out.readValuesToList.asScala.toList
      )
    )

  def run(input: (KA, A)*): F[Map[KB, B]] =
    for {
      topicIn <- randomString
      topicOut <- randomString
      topo <- topology(topicIn, topicOut)
      bs <- testDriverResource(topo).use(driver => runTestTopologyMap(topicIn, topicOut, driver, input))
    } yield bs

  def runList(input: A*)(implicit ev: String =:= KA): F[List[B]] =
    for {
      topicIn <- randomString
      topicOut <- randomString
      topo <- topology(topicIn, topicOut)
      bs <- testDriverResource(topo).use(driver =>
        runTestTopologyValues(topicIn, topicOut, driver, input.toList.tupleLeft(ev("key")))
      )
    } yield bs

  def topology(topic: String, outputTopic: String): F[Topology] = {
    val sb = new StreamsBuilder
    f(STable.withLogCompaction[C, KA, A](sb, topic)).to[F](outputTopic) >> Sync[F].delay(sb.build)
  }

  def props = {
    val p = new ju.Properties
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkastreams4s-test")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }
}

object KafkaStreamsTestRunner {

  def run[F[_]: Sync, C[_]: Codec, KA: C, A: C, KB: C, B: C](
    f: STable[C, KA, A] => STable[C, KB, B],
    input: (KA, A)*
  ): F[Map[KB, B]] =
    new KafkaStreamsTestRunner[F, C, KA, A, KB, B](f).run(input: _*)

  def runList[F[_]: Sync, C[_]: Codec, A: C, B: C](
    f: STable[C, String, A] => STable[C, String, B],
    input: A*
  )(implicit C: C[String]): F[List[B]] =
    new KafkaStreamsTestRunner[F, C, String, A, String, B](f).runList(input: _*)
}
