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

object JoinTables {

  def join[K1, K2: DebeziumType, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.join(
      b,
      v1 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload(f(v1), idName)),
      (v1, v2) => g(v1, v2)
    )

  def joinOption[K1, K2: DebeziumType, V1, V2, Z](
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
          .map(k2 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload(k2, idName)))
          .orNull,
      (v1, v2) => g(v1, v2)
    )

  def leftJoin[K1, K2: DebeziumType, V1, V2, Z](
    a: KTable[K1, V1],
    b: KTable[DebeziumKey[K2], V2],
    idName: String,
    topicName: String
  )(
    f: V1 => K2
  )(g: (V1, V2) => Z): KTable[K1, Z] =
    a.leftJoin(
      b,
      v1 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload(f(v1), idName)),
      (v1, v2) => g(v1, v2)
    )

  def leftJoinOption[K1, K2: DebeziumType, V1, V2, Z](
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
          .map(k2 => DebeziumKey(replicateJsonKeySchema[K2](idName, topicName), DebeziumKeyPayload(k2, idName)))
          .orNull,
      (v1, v2) => g(v1, v2)
    )

  private[kafkastreams4s] def replicateJsonKeySchema[A: DebeziumType](idName: String, topicName: String): JsonObject =
    JsonObject(
      "type" -> "struct".asJson,
      "fields" -> List(
        Json.obj("type" -> DebeziumType[A].debeziumType.asJson, "optional" -> Json.False, "field" -> idName.asJson)
      ).asJson,
      "optional" -> Json.False,
      "name" -> s"$topicName.Key".asJson
    )
}
