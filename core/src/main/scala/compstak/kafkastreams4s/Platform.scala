package compstak.kafkastreams4s

import cats.effect._
import cats.syntax.all._
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.{KafkaStreams, Topology}

import java.time.Duration
import java.util.Properties

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
  def run[F[_]: Concurrent: Sync: Async](top: Topology, props: Properties, timeout: Duration): F[ShutdownStatus] =
    for {
      d <- Deferred[F, Boolean]
      r <- Resource
        .make(
          Sync[F].delay(new KafkaStreams(top, props))
        )(s => Sync[F].delay(s.close(timeout)).flatMap(b => d.complete(b) >> Sync[F].unit))
        .use { streams =>
          Async[F].async_ { (k: Either[Throwable, Unit] => Unit) =>
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
