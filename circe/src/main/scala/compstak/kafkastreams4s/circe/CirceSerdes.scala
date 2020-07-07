package compstak.kafkastreams4s.circe

import io.circe.parser.decode
import io.circe.{Decoder, Encoder}
import io.circe.syntax._
import cats.implicits._
import org.apache.kafka.common.serialization.{
  Deserializer,
  Serde,
  Serdes,
  Serializer,
  StringDeserializer,
  StringSerializer
}
import compstak.kafkastreams4s.SerdeHelpers
import compstak.kafkastreams4s.implicits._

object CirceSerdes {

  private val stringSerializer: Serializer[String] = new StringSerializer()
  private val stringDeserializer: Deserializer[String] = new StringDeserializer()

  def serializerForCirce[A: Encoder]: Serializer[A] =
    stringSerializer.contramap(_.asJson.noSpaces)

  def deserializerForCirce[A: Decoder]: Deserializer[A] =
    SerdeHelpers.emapDeserializer(stringDeserializer)(decode[A](_).leftMap(_.toString))

  def serdeForCirce[A: Encoder: Decoder]: Serde[A] =
    Serdes.serdeFrom(serializerForCirce, deserializerForCirce)
}
