package compstak.kafkastreams4s.debezium

import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax._
import io.circe.parser.decode
import cats.implicits._
import compstak.circe.debezium.DebeziumKeyPayload
import compstak.circe.debezium.DebeziumKey
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import io.circe.JsonObject
import compstak.circe.debezium.DebeziumKeySchema
import compstak.circe.debezium.DebeziumFieldSchema
import compstak.circe.debezium.DebeziumSchemaPrimitive

object JoinTables {

  def joinComposite[K1, K2, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    schema: DebeziumCompositeType[K2],
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.join(
      b,
      v1 =>
        DebeziumKey(replicateCompositeKeySchema[K2](schema, topicName), DebeziumKeyPayload.CompositeKeyPayload(f(v1))),
      (v1, v2) => g(v1, v2)
    )

  def joinOptionComposite[K1, K2, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    schema: DebeziumCompositeType[K2],
    topicName: String
  )(
    f: V1 => Option[K2]
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.join(
      b,
      v1 =>
        f(v1)
          .map(k2 =>
            DebeziumKey(replicateCompositeKeySchema[K2](schema, topicName), DebeziumKeyPayload.CompositeKeyPayload(k2))
          )
          .orNull,
      (v1, v2) => g(v1, v2)
    )

  def leftJoinComposite[K1, K2, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    schema: DebeziumCompositeType[K2],
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.leftJoin(
      b,
      v1 =>
        DebeziumKey(replicateCompositeKeySchema[K2](schema, topicName), DebeziumKeyPayload.CompositeKeyPayload(f(v1))),
      (v1, v2) => g(v1, v2)
    )

  def leftJoinOptionComposite[K1, K2, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    schema: DebeziumCompositeType[K2],
    topicName: String
  )(
    f: V1 => Option[K2]
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.leftJoin(
      b,
      v1 =>
        f(v1)
          .map(k2 =>
            DebeziumKey(replicateCompositeKeySchema[K2](schema, topicName), DebeziumKeyPayload.CompositeKeyPayload(k2))
          )
          .orNull,
      (v1, v2) => g(v1, v2)
    )

  def join[K1, K2: DebeziumPrimitiveType, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.join(
      b,
      v1 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload.simple(f(v1), idName)),
      (v1, v2) => g(v1, v2)
    )

  def joinOption[K1, K2: DebeziumPrimitiveType, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => Option[K2]
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.join(
      b,
      v1 =>
        f(v1)
          .map(k2 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload.simple(k2, idName)))
          .orNull,
      (v1, v2) => g(v1, v2)
    )

  def leftJoin[K1, K2: DebeziumPrimitiveType, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.leftJoin(
      b,
      v1 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload.simple(f(v1), idName)),
      (v1, v2) => g(v1, v2)
    )

  def leftJoinOption[K1, K2: DebeziumPrimitiveType, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => Option[K2]
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.leftJoin(
      b,
      v1 =>
        f(v1)
          .map(k2 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload.simple(k2, idName)))
          .orNull,
      (v1, v2) => g(v1, v2)
    )

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
