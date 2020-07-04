package compstak.kafkastreams4s.circe

import io.circe.parser.decode
import io.circe.{Decoder, Encoder}
import io.circe.syntax._
import cats.implicits._
import org.apache.kafka.common.serialization.{Serde, Serdes}
import compstak.kafkastreams4s.SerdeHelpers

object CirceSerdes {
  def serdeForCirce[A: Encoder: Decoder]: Serde[A] =
    SerdeHelpers.emap(Serdes.String())(s => decode[A](s).leftMap(_.toString))(_.asJson.noSpaces)
}
