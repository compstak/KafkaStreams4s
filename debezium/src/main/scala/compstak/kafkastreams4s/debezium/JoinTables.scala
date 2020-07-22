package compstak.kafkastreams4s.debezium

import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax._
import io.circe.parser.decode
import cats.implicits._
import compstak.circe.debezium._
import compstak.kafkastreams4s.circe.CirceSerdes
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.common.serialization.{Serde, Serdes}
import compstak.kafkastreams4s.circe.CirceTable

object JoinTables {

  def joinComposite[K1: Encoder: Decoder, K2, V1, V2, Z: Encoder: Decoder](
    a: CirceTable[K1, V1],
    b: CirceTable[DebeziumKey[K2], V2],
    schema: DebeziumCompositeType[K2],
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): CirceTable[K1, Z] =
    a.join(b)(v1 =>
      DebeziumKey(replicateCompositeKeySchema[K2](schema, topicName), DebeziumKeyPayload.CompositeKeyPayload(f(v1)))
    )(g)

  def joinOptionComposite[K1: Encoder: Decoder, K2, V1, V2, Z: Encoder: Decoder](
    a: CirceTable[K1, V1],
    b: CirceTable[DebeziumKey[K2], V2],
    schema: DebeziumCompositeType[K2],
    topicName: String
  )(
    f: V1 => Option[K2]
  )(g: (V1, V2) => Z): CirceTable[K1, Z] =
    a.join(b)(v1 =>
      f(v1)
        .map(k2 =>
          DebeziumKey(replicateCompositeKeySchema[K2](schema, topicName), DebeziumKeyPayload.CompositeKeyPayload(k2))
        )
        .orNull
    )(g)

  def leftJoinComposite[K1: Encoder: Decoder, K2, V1, V2, Z: Encoder: Decoder](
    a: CirceTable[K1, V1],
    b: CirceTable[DebeziumKey[K2], V2],
    schema: DebeziumCompositeType[K2],
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): CirceTable[K1, Z] =
    a.leftJoin(b)(v1 =>
      DebeziumKey(replicateCompositeKeySchema[K2](schema, topicName), DebeziumKeyPayload.CompositeKeyPayload(f(v1)))
    )(g)

  def leftJoinOptionComposite[K1: Encoder: Decoder, K2, V1, V2, Z: Encoder: Decoder](
    a: CirceTable[K1, V1],
    b: CirceTable[DebeziumKey[K2], V2],
    schema: DebeziumCompositeType[K2],
    topicName: String
  )(
    f: V1 => Option[K2]
  )(g: (V1, V2) => Z): CirceTable[K1, Z] =
    a.leftJoin(b)(v1 =>
      f(v1)
        .map(k2 =>
          DebeziumKey(replicateCompositeKeySchema[K2](schema, topicName), DebeziumKeyPayload.CompositeKeyPayload(k2))
        )
        .orNull
    )(g)

  def join[K1: Encoder: Decoder, K2: DebeziumPrimitiveType, V1, V2, Z: Encoder: Decoder](
    a: CirceTable[K1, V1],
    b: CirceTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): CirceTable[K1, Z] =
    a.join(b)(v1 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload.simple(f(v1), idName))
    )(g)

  def joinOption[K1: Encoder: Decoder, K2: DebeziumPrimitiveType, V1, V2, Z: Encoder: Decoder](
    a: CirceTable[K1, V1],
    b: CirceTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => Option[K2]
  )(g: (V1, V2) => Z): CirceTable[K1, Z] =
    a.joinOption(b)(v1 =>
      f(v1)
        .map(k2 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload.simple(k2, idName)))
    )(g)

  def leftJoin[K1: Encoder: Decoder, K2: DebeziumPrimitiveType, V1, V2, Z: Encoder: Decoder](
    a: CirceTable[K1, V1],
    b: CirceTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): CirceTable[K1, Z] =
    a.leftJoin(b)(v1 =>
      DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload.simple(f(v1), idName))
    )(g)

  def leftJoinOption[K1: Encoder: Decoder, K2: DebeziumPrimitiveType, V1, V2, Z: Encoder: Decoder](
    a: CirceTable[K1, V1],
    b: CirceTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => Option[K2]
  )(g: (V1, V2) => Z): CirceTable[K1, Z] =
    a.leftJoinOption(b)(v1 =>
      f(v1)
        .map(k2 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload.simple(k2, idName)))
    )(g)

  private[kafkastreams4s] def replicateCompositeKeySchema[A](
    ct: DebeziumCompositeType[A],
    topicName: String
  ): DebeziumKeySchema =
    DebeziumKeySchema(ct.schema, s"$topicName.Key")

  private[kafkastreams4s] def replicateJsonKeySchema[A: DebeziumPrimitiveType](
    idName: String,
    topicName: String
  ): DebeziumKeySchema =
    DebeziumKeySchema(
      List(
        DebeziumFieldSchema(DebeziumPrimitiveType[A].debeziumType, false, idName)
      ),
      s"$topicName.Key"
    )

}
