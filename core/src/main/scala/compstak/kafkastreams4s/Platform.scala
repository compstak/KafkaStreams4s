package compstak.kafkastreams4s

import cats._, cats.implicits._
import cats.effect.{Async, Concurrent, ExitCase, ExitCode, Resource, Sync}
import cats.effect.implicits._
import cats.effect.concurrent.Deferred
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.KafkaStreams.State
import java.util.Properties
import java.time.Duration
import org.apache.kafka.common.protocol.types.Field.Bool

sealed trait ShutdownStatus
final case object ShutdownComplete extends ShutdownStatus
final case object ShutdownIncomplete extends ShutdownStatus

object ShutdownStatus {
  def apply(shutdown: Boolean): ShutdownStatus =
    if (shutdown) {
      ShutdownComplete
    } else {
      ShutdownIncomplete
    }
}

object Platform {
  def run[F[_]: Concurrent](top: Topology, props: Properties, timeout: Duration): F[ShutdownStatus] =
    for {
      d <- Deferred[F, Boolean]
      r <- Resource
        .make(
          Sync[F].delay(new KafkaStreams(top, props))
        )(s => Sync[F].delay(s.close(timeout)).flatMap(b => d.complete(b)))
        .use { streams =>
          Async[F].async { (k: Either[Throwable, Unit] => Unit) =>
            streams.setUncaughtExceptionHandler { (_: Thread, e: Throwable) =>
              k(Left(e))
            }

            streams.setStateListener { (state: State, _: State) =>
              state match {
                case State.ERROR =>
                  k(Left(new RuntimeException("The KafkaStreams went into an ERROR state.")))
                case _ => ()
              }
            }

            streams.start()
          }
        }
        .redeemWith(_ => d.get, _ => d.get)
    } yield ShutdownStatus(r)
}
