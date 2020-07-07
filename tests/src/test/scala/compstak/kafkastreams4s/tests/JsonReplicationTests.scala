package compstak.kafkastreams4s

import compstak.kafkastreams4s.debezium.JoinTables
import compstak.circe.debezium.DebeziumKey
import compstak.circe.debezium.DebeziumKeyPayload
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
    val key = DebeziumKey(keySchema, DebeziumKeyPayload(1, idName))

    assertEquals(key.asJson.noSpaces, expectedKeyString)

    assertEquals(
      CirceSerdes.serializerForCirce[DebeziumKey[Int]].serialize(topicName, key).toList,
      Serdes.String().serializer.serialize(topicName, expectedKeyString).toList
    )
  }

}
