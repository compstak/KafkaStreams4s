package compstak.kafkastreams4s.testing

import org.apache.kafka.streams.Topology
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

  import KafkaStreamsTestRunner._

  private def randomString: F[String] = Sync[F].delay(Random.alphanumeric.take(32).mkString)

  def run(input: (KA, A)*): F[Map[KB, B]] =
    for {
      topicIn <- randomString
      topicOut <- randomString
      topo <- topology(topicIn, topicOut)
      bs <- testDriverResource[F](topo).use(driver =>
        inputTestTable[F, C](driver, topicIn, input: _*) >> outputTestTable[F, C, KB, B](driver, topicOut)
      )
    } yield bs

  def runList(input: A*)(implicit ev: String =:= KA): F[List[B]] =
    for {
      topicIn <- randomString
      topicOut <- randomString
      topo <- topology(topicIn, topicOut)
      bs <- testDriverResource[F](topo).use(driver =>
        inputTestTable[F, C](driver, topicIn, input.toList.tupleLeft(ev("key")): _*) >>
          outputTestTableList[F, C, KB, B](driver, topicOut)
      )
    } yield bs

  def topology(topic: String, outputTopic: String): F[Topology] = {
    val sb = new StreamsBuilder
    f(STable[C, KA, A](sb, topic)).toRemoveNulls[F](outputTopic) >> Sync[F].delay(sb.build)
  }

}

object KafkaStreamsTestRunner {

  def testDriverResource[F[_]: Sync](topo: Topology): Resource[F, TopologyTestDriver] =
    Resource.make(Sync[F].delay(new TopologyTestDriver(topo, props)))(d => Sync[F].delay(d.close))

  def inputTestTable[F[_], C[_]]: InputPartiallyAppliedF[F, C] = new InputPartiallyAppliedF

  private[kafkastreams4s] class InputPartiallyAppliedF[F[_], C[_]](private val dummy: Unit = ()) extends AnyVal {
    def apply[K: C, V: C](
      driver: TopologyTestDriver,
      name: String,
      input: (K, V)*
    )(implicit C: Codec[C], F: Sync[F]): F[Unit] = {
      val in = driver.createInputTopic(name, Codec[C].serde[K].serializer, Codec[C].serde[V].serializer)
      input.toList.traverse_ { case (k, v) => F.delay(in.pipeInput(k, v)) }
    }
  }

  def outputTestTable[F[_]: Sync, C[_]: Codec, K: C, V: C](driver: TopologyTestDriver, name: String): F[Map[K, V]] = {
    val out = driver.createOutputTopic(name, Codec[C].serde[K].deserializer, Codec[C].serde[V].deserializer)
    Sync[F].delay(out.readKeyValuesToMap.asScala.toMap)
  }

  def outputTestTableList[F[_]: Sync, C[_]: Codec, K: C, V: C](driver: TopologyTestDriver, name: String): F[List[V]] = {
    val out = driver.createOutputTopic(name, Codec[C].serde[K].deserializer, Codec[C].serde[V].deserializer)
    Sync[F].delay(out.readValuesToList.asScala.toList)
  }

  def props: ju.Properties = {
    val p = new ju.Properties
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkastreams4s-test")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

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
