package compstak.kafkastreams4s.debezium

import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax._
import io.circe.parser.decode
import cats.implicits._
import compstak.circe.debezium._
import compstak.kafkastreams4s.circe.CirceSerdes
import org.apache.kafka.streams.kstream.{KTable, Materialized}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes

object JoinTables {

  def joinComposite[K1: Encoder: Decoder, K2, V1, V2, Z: Encoder: Decoder](
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
      (v1, v2) => g(v1, v2),
      Materialized
        .`with`[K1, Z, KeyValueStore[Bytes, Array[Byte]]](CirceSerdes.serdeForCirce[K1], CirceSerdes.serdeForCirce[Z])
    )

  def joinOptionComposite[K1: Encoder: Decoder, K2, V1, V2, Z: Encoder: Decoder](
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
      (v1, v2) => g(v1, v2),
      Materialized
        .`with`[K1, Z, KeyValueStore[Bytes, Array[Byte]]](CirceSerdes.serdeForCirce[K1], CirceSerdes.serdeForCirce[Z])
    )

  def leftJoinComposite[K1: Encoder: Decoder, K2, V1, V2, Z: Encoder: Decoder](
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
      (v1, v2) => g(v1, v2),
      Materialized
        .`with`[K1, Z, KeyValueStore[Bytes, Array[Byte]]](CirceSerdes.serdeForCirce[K1], CirceSerdes.serdeForCirce[Z])
    )

  def leftJoinOptionComposite[K1: Encoder: Decoder, K2, V1, V2, Z: Encoder: Decoder](
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
      (v1, v2) => g(v1, v2),
      Materialized
        .`with`[K1, Z, KeyValueStore[Bytes, Array[Byte]]](CirceSerdes.serdeForCirce[K1], CirceSerdes.serdeForCirce[Z])
    )

  def join[K1: Encoder: Decoder, K2: DebeziumPrimitiveType, V1, V2, Z: Encoder: Decoder](
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
      (v1, v2) => g(v1, v2),
      Materialized
        .`with`[K1, Z, KeyValueStore[Bytes, Array[Byte]]](CirceSerdes.serdeForCirce[K1], CirceSerdes.serdeForCirce[Z])
    )

  def joinOption[K1: Encoder: Decoder, K2: DebeziumPrimitiveType, V1, V2, Z: Encoder: Decoder](
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
      (v1, v2) => g(v1, v2),
      Materialized
        .`with`[K1, Z, KeyValueStore[Bytes, Array[Byte]]](CirceSerdes.serdeForCirce[K1], CirceSerdes.serdeForCirce[Z])
    )

  def leftJoin[K1: Encoder: Decoder, K2: DebeziumPrimitiveType, V1, V2, Z: Encoder: Decoder](
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
      (v1, v2) => g(v1, v2),
      Materialized
        .`with`[K1, Z, KeyValueStore[Bytes, Array[Byte]]](CirceSerdes.serdeForCirce[K1], CirceSerdes.serdeForCirce[Z])
    )

  def leftJoinOption[K1: Encoder: Decoder, K2: DebeziumPrimitiveType, V1, V2, Z: Encoder: Decoder](
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
      (v1, v2) => g(v1, v2),
      Materialized
        .`with`[K1, Z, KeyValueStore[Bytes, Array[Byte]]](CirceSerdes.serdeForCirce[K1], CirceSerdes.serdeForCirce[Z])
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
