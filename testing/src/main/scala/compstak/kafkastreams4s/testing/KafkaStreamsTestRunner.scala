package compstak.kafkastreams4s.testing

import org.apache.kafka.streams.Topology

import scala.util.Random
import cats.effect.Sync
import cats.implicits._
import cats.effect.implicits._
import java.time.Duration
import java.util.UUID
import java.{util => ju}

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.StreamsBuilder

import scala.concurrent.duration._
import org.apache.kafka.streams.TopologyTestDriver
import cats.effect.Resource

import scala.collection.JavaConverters._
import compstak.kafkastreams4s._
import org.apache.kafka.streams.TestOutputTopic

private[kafkastreams4s] class KafkaStreamsTestRunner[F[_]: Sync, HK[_]: Codec, HV[
  _
]: Codec, KA: HK, A: HV, KB: HK, B: HV](
  f: STable[HK, KA, HV, A] => STable[HK, KB, HV, B]
) {

  import KafkaStreamsTestRunner._

  private def randomString: F[String] = Sync[F].delay(Random.alphanumeric.take(32).mkString)

  def run(input: (KA, A)*): F[Map[KB, B]] =
    for {
      topicIn <- randomString
      topicOut <- randomString
      topo <- topology(topicIn, topicOut)
      bs <- testDriverResource[F](topo).use(driver =>
        inputTestTable2[F, HK, HV](driver, topicIn, input: _*) >> outputTestTable2[F, HK, KB, HV, B](driver, topicOut)
      )
    } yield bs

  def runList(input: A*)(implicit ev: String =:= KA): F[List[B]] =
    for {
      topicIn <- randomString
      topicOut <- randomString
      topo <- topology(topicIn, topicOut)
      bs <- testDriverResource[F](topo).use(driver =>
        inputTestTable2[F, HK, HV](driver, topicIn, input.toList.tupleLeft(ev("key")): _*) >>
          outputTestTableList2[F, HK, KB, HV, B](driver, topicOut)
      )
    } yield bs

  def topology(topic: String, outputTopic: String): F[Topology] = {
    val sb = new StreamsBuilder
    f(STable[HK, KA, HV, A](sb, topic)).toRemoveNulls[F](outputTopic) >> Sync[F].delay(sb.build)
  }

}

object KafkaStreamsTestRunner {

  def testDriverResource[F[_]: Sync](topo: Topology): Resource[F, TopologyTestDriver] =
    Resource.make(props.map(p => new TopologyTestDriver(topo, p)))(d => Sync[F].delay(d.close))

  def inputTestTable2[F[_], HK[_], HV[_]]: InputPartiallyAppliedF[F, HK, HV] = new InputPartiallyAppliedF[F, HK, HV]

  def inputTestTable[F[_], C[_]]: InputPartiallyAppliedF[F, C, C] = new InputPartiallyAppliedF[F, C, C]

  private[kafkastreams4s] class InputPartiallyAppliedF[F[_], HK[_], HV[_]](private val dummy: Unit = ())
      extends AnyVal {
    def apply[K: HK, V: HV](
      driver: TopologyTestDriver,
      name: String,
      input: (K, V)*
    )(implicit C1: Codec[HK], C2: Codec[HV], F: Sync[F]): F[Unit] = {
      val in = driver.createInputTopic(name, Codec[HK].serde[K].serializer, Codec[HV].serde[V].serializer)
      input.toList.traverse_ { case (k, v) => F.delay(in.pipeInput(k, v)) }
    }
  }

  def outputTestTable2[F[_]: Sync, HK[_]: Codec, K: HK, HV[_]: Codec, V: HV](
    driver: TopologyTestDriver,
    name: String
  ): F[Map[K, V]] = {
    val out = driver.createOutputTopic(name, Codec[HK].serde[K].deserializer, Codec[HV].serde[V].deserializer)
    Sync[F].delay(out.readKeyValuesToMap.asScala.toMap)
  }

  def outputTestTable[F[_]: Sync, C[_]: Codec, K: C, V: C](
    driver: TopologyTestDriver,
    name: String
  ): F[Map[K, V]] = {
    val out = driver.createOutputTopic(name, Codec[C].serde[K].deserializer, Codec[C].serde[V].deserializer)
    Sync[F].delay(out.readKeyValuesToMap.asScala.toMap)
  }

  def outputTestTableList2[F[_]: Sync, HK[_]: Codec, K: HK, HV[_]: Codec, V: HV](
    driver: TopologyTestDriver,
    name: String
  ): F[List[V]] = {
    val out = driver.createOutputTopic(name, Codec[HK].serde[K].deserializer, Codec[HV].serde[V].deserializer)
    Sync[F].delay(out.readValuesToList.asScala.toList)
  }

  def outputTestTableList2[F[_]: Sync, C[_]: Codec, K: C, V: C](
    driver: TopologyTestDriver,
    name: String
  ): F[List[V]] = {
    val out = driver.createOutputTopic(name, Codec[C].serde[K].deserializer, Codec[C].serde[V].deserializer)
    Sync[F].delay(out.readValuesToList.asScala.toList)
  }

  def props[F[_]: Sync]: F[ju.Properties] =
    Sync[F].delay {
      val p = new ju.Properties
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p
    }

  def run2[F[_]: Sync, HK[_]: Codec, HV[_]: Codec, KA: HK, A: HV, KB: HK, B: HV](
    f: STable[HK, KA, HV, A] => STable[HK, KB, HV, B],
    input: (KA, A)*
  ): F[Map[KB, B]] =
    new KafkaStreamsTestRunner[F, HK, HV, KA, A, KB, B](f).run(input: _*)

  def run[F[_]: Sync, C[_]: Codec, KA: C, A: C, KB: C, B: C](
    f: STable[C, KA, C, A] => STable[C, KB, C, B],
    input: (KA, A)*
  ): F[Map[KB, B]] =
    new KafkaStreamsTestRunner[F, C, C, KA, A, KB, B](f).run(input: _*)

  def runList[F[_]: Sync, C[_]: Codec, A: C, B: C](
    f: STable[C, String, C, A] => STable[C, String, C, B],
    input: A*
  )(implicit C: C[String]): F[List[B]] =
    new KafkaStreamsTestRunner[F, C, C, String, A, String, B](f).runList(input: _*)
}
