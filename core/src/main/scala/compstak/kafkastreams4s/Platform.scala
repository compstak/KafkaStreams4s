package compstak.kafkastreams4s

import cats.effect.{Async, Resource, Sync}
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.KafkaStreams.State
import java.util.Properties
import java.time.Duration

object Platform {
  def streamsResource[F[_]: Sync](top: Topology, props: Properties, timeout: Duration): Resource[F, KafkaStreams] =
    Resource.make(Sync[F].delay(new KafkaStreams(top, props)))(s => Sync[F].delay(s.close(timeout)))

  def runStreams[F[_]: Async](streams: KafkaStreams): F[Unit] = Async[F].async { (k: Either[Throwable, Unit] => Unit) =>
    streams.setUncaughtExceptionHandler { (_: Thread, e: Throwable) =>
      k(Left(e))
    }

    streams.setStateListener { (state: State, _: State) =>
      state match {
        case State.ERROR => k(Left(new RuntimeException("The KafkaStreams went into an ERROR state.")))
        case _ => ()
      }
    }

    streams.start()
  }

  def run[F[_]: Async](topo: Topology, props: Properties, timeout: Duration): F[Unit] =
    streamsResource[F](topo, props, timeout).use(runStreams[F])
}
