package compstak.kafkastreams4s.vulcan

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore

object VulcanSerdes {
  def serdeForVulcan[A : VulcanCodec]: Serde[A] = new GenericSerde[A]

  def producedForVulcan[K : VulcanCodec, V : VulcanCodec]
  : Produced[K, V] =
    Produced.`with`(serdeForVulcan[K], serdeForVulcan[V])

  def materializedForVulcan[K : VulcanCodec, V : VulcanCodec]
  : Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    Materialized
      .`with`(serdeForVulcan[K], serdeForVulcan[V])

  def consumedForVulcan[K : VulcanCodec, V : VulcanCodec]
  : Consumed[K, V] =
    Consumed.`with`(serdeForVulcan[K], serdeForVulcan[V])

  def groupedForVulcan[K : VulcanCodec, V : VulcanCodec]: Grouped[K, V] =
    Grouped.`with`(serdeForVulcan[K], serdeForVulcan[V])

}