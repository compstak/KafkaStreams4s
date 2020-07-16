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
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes
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

  def producedForCirce[K: Encoder: Decoder, V: Encoder: Decoder]: Produced[K, V] =
    Produced.`with`(serdeForCirce[K], serdeForCirce[V])

  def materializedForCirce[K: Encoder: Decoder, V: Encoder: Decoder]
    : Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    Materialized
      .`with`(serdeForCirce[K], serdeForCirce[V])

  def consumedForCirce[K: Encoder: Decoder, V: Encoder: Decoder]: Consumed[K, V] =
    Consumed.`with`(serdeForCirce[K], serdeForCirce[V])

  def groupedForCirce[K: Encoder: Decoder, V: Encoder: Decoder]: Grouped[K, V] =
    Grouped.`with`(serdeForCirce[K], serdeForCirce[V])
}
