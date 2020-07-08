package compstak.kafkastreams4s.tests

import cats.effect.{Blocker, IO, Resource}
import cats.implicits._
import org.http4s.implicits._
import org.http4s.client.asynchttpclient.AsyncHttpClient
import doobie.{ConnectionIO, Transactor}
import doobie.implicits._
import doobie.free.driver.DriverOp.Connect
import doobie.util.ExecutionContexts
import io.circe.Decoder
import io.circe.literal._
import fs2.kafka._
import compstak.circe.debezium.{DebeziumKey, DebeziumValue}
import compstak.http4s.kafka.connect.KafkaConnectMigration
import compstak.kafkastreams4s.circe.CirceSerdes
import compstak.kafkastreams4s.Platform
import compstak.kafkastreams4s.debezium.DebeziumTable
import org.apache.kafka.streams.StreamsBuilder
import io.circe.Encoder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{Consumed, KTable, Produced}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import java.time.Duration
import java.{util => ju}
import org.apache.kafka.common.serialization.Serdes
import cats.effect.ExitCode

class EndToEndTests extends munit.FunSuite {

  override val munitTimeout = 3.minutes

  implicit val ctx = IO.contextShift(ExecutionContext.global)
  implicit val timer = IO.timer(ExecutionContext.global)

  val kafkaHost = "localhost:9092"
  val outputTopic = "output.topic"
  val (foo, bar) = ("foo", "bar")

  val username = "postgres"
  val password = ""
  val database = "postgres"

  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    s"jdbc:postgresql://localhost:54320/postgres",
    username,
    password,
    Blocker.liftExecutionContext(ExecutionContexts.synchronous)
  )

  def make: Resource[IO, Unit] =
    for {
      _ <- Resource.liftF(ddl.transact(xa))
      client <- AsyncHttpClient.resource[IO]()
      _ <- KafkaConnectMigration[IO](
        client,
        uri"http://localhost:18083",
        Map(
          "kafkastreams4s-test" -> json"""
          {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "plugin.name":"pgoutput",
            "database.hostname": "database",
            "database.port": "5432",
            "database.user": $username,
            "database.password": $password,
            "database.dbname" : $database,
            "database.server.name": "experiment",
            "table.whitelist": "public.atable, public.btable"
          }
          """
        ),
        "experiment"
      ).evalMap(_.migrate)
      _ <- Resource.liftF(insertStmt.transact(xa))
      // run the kafka streams topology for a minute and then stop it
      _ <- Resource.liftF(
        (
          KafkaStream.run,
          IO.sleep(1.minutes)
        ).parTupled.void
          .timeout(1.minute)
          .recoverWith { case t: java.util.concurrent.TimeoutException => IO.unit }
      )
    } yield ()

  def ddl: ConnectionIO[Unit] = sql"""

    CREATE TABLE IF NOT EXISTS atable (
      id SERIAL PRIMARY KEY,
      foo TEXT
    );

    CREATE TABLE IF NOT EXISTS btable (
      id SERIAL PRIMARY KEY,
      a_id INTEGER REFERENCES atable (id),
      bar TEXT
    );
  """.update.run.void

  def insertStmt: ConnectionIO[Unit] =
    sql"""
      WITH a AS (
        INSERT INTO atable (foo) VALUES ($foo) RETURNING id
      )
      INSERT INTO btable (a_id, bar) VALUES ((SELECT id FROM a), $bar);
    """.update.run.void

  test("Joins two debezium streams and aggregates the result") {
    make
      .use(_ => Consumer.consume.timeout(2.minute).map(assertEquals(_, (foo, bar))))
      .unsafeToFuture()
  }

  object KafkaStream {
    val props = {
      val p = new ju.Properties
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkastreams4s-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
      p
    }

    def topology = {
      val sb = new StreamsBuilder

      val as = DebeziumTable.withCirceDebezium[Int, Atable](sb, "experiment.public.atable", "id")
      val bs = DebeziumTable.withCirceDebezium[Int, Btable](sb, "experiment.public.btable", "id")

      val output: KTable[DebeziumKey[Int], (String, String)] =
        bs.joinOption(as)(extractAId)(valueJoiner).toKTable

      output
        .toStream()
        .to(
          outputTopic,
          Produced.`with`(CirceSerdes.serdeForCirce[DebeziumKey[Int]], CirceSerdes.serdeForCirce[(String, String)])
        )
      sb.build()
    }

    def extractAId(d: DebeziumValue[Btable]): Option[Int] =
      d.payload.after.map(_.a_id)

    def valueJoiner(b: DebeziumValue[Btable], a: DebeziumValue[Atable]): (String, String) =
      (a.payload.after.foldMap(_.foo), b.payload.after.foldMap(_.bar))

    def run: IO[Unit] =
      Platform.streamsResource[IO](topology, props, Duration.ofSeconds(2)).use(Platform.runStreams[IO])
  }

  object Consumer {

    implicit def fs2KafkaDeserializer[A: Decoder]: Deserializer[IO, A] =
      Deserializer.delegate[IO, A](CirceSerdes.deserializerForCirce).suspend

    val settings = ConsumerSettings[IO, DebeziumKey[Int], (String, String)]
      .withAllowAutoCreateTopics(true)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers(kafkaHost)
      .withGroupId("group")

    def consume: IO[(String, String)] =
      consumerStream(settings)
        .evalTap(_.subscribeTo(outputTopic))
        .flatMap(c => c.stream)
        .take(1)
        .map(_.record.value)
        .compile
        .lastOrError

  }
}

case class Atable(id: Int, foo: String)
object Atable {
  implicit val decoder: Decoder[Atable] = Decoder.forProduct2("id", "foo")(Atable.apply)
  implicit val encoder: Encoder[Atable] = Encoder.forProduct2("id", "foo")(a => (a.id, a.foo))
}

case class Btable(id: Int, a_id: Int, bar: String)
object Btable {
  implicit val decoder: Decoder[Btable] = Decoder.forProduct3("id", "a_id", "bar")(Btable.apply)
  implicit val encoder: Encoder[Btable] = Encoder.forProduct3("id", "a_id", "bar")(b => (b.id, b.a_id, b.bar))
}
