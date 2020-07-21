package compstak.kafkastreams4s

import compstak.kafkastreams4s.debezium.{DebeziumCompositeType, JoinTables}
import compstak.circe.debezium._
import io.circe.syntax._
import io.circe.Encoder
import compstak.kafkastreams4s.circe.CirceSerdes
import org.apache.kafka.common.serialization.Serdes

class JsonReplicationTests extends munit.FunSuite {

  test("Fully duplicates debezium json key string") {
    val expectedKeyString =
      """{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"}],"optional":false,"name":"experiment.public.atable.Key"},"payload":{"id":1}}"""

    val idName = "id"
    val topicName = "experiment.public.atable"

    val keySchema = JoinTables.replicateJsonKeySchema[Int](idName, topicName)
    val key = DebeziumKey(keySchema, DebeziumKeyPayload.simple(1, idName))

    assertEquals(key.asJson.noSpaces, expectedKeyString)

    assertEquals(
      CirceSerdes.serializerForCirce[DebeziumKey[Int]].serialize(topicName, key).toList,
      Serdes.String().serializer.serialize(topicName, expectedKeyString).toList
    )
  }

  test("Fully duplicates debezium composite key string") {
    val expectedKeyString =
      """{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"data"}],"optional":false,"name":"experiment.public.atable.Key"},"payload":{"id":43,"data":"foo"}}"""

    case class Atable(id: Int, data: String)

    implicit val encoder: Encoder[Atable] = Encoder.forProduct2("id", "data")(a => (a.id, a.data))

    val topicName = "experiment.public.atable"
    val keyFields = new compstak.kafkastreams4s.debezium.DebeziumCompositeType[Atable] {
      def schema =
        List(
          DebeziumFieldSchema(DebeziumSchemaPrimitive.Int32, false, "id"),
          DebeziumFieldSchema(DebeziumSchemaPrimitive.String, false, "data")
        )
    }

    val keySchema = JoinTables.replicateCompositeKeySchema(keyFields, topicName)
    val key = DebeziumKey(keySchema, DebeziumKeyPayload.CompositeKeyPayload(Atable(43, "foo")))

    assertEquals(key.asJson.noSpaces, expectedKeyString)

    assertEquals(
      CirceSerdes.serializerForCirce[DebeziumKey[Atable]].serialize(topicName, key).toList,
      Serdes.String().serializer.serialize(topicName, expectedKeyString).toList
    )
  }
}
