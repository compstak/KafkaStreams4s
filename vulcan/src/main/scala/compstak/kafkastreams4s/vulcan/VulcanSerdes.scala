package compstak.kafkastreams4s.vulcan

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import vulcan.{Codec => VCodec}

object VulcanSerdes {
  def serdeForVulcan[A: VCodec]: Serde[A] = new VulcanSerdeCreation[A]

  def producedForVulcan[K: VCodec, V: VCodec]: Produced[K, V] =
    Produced.`with`(serdeForVulcan[K], serdeForVulcan[V])

  def materializedForVulcan[K: VCodec, V: VCodec]: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    Materialized
      .`with`(serdeForVulcan[K], serdeForVulcan[V])

  def consumedForVulcan[K: VCodec, V: VCodec]: Consumed[K, V] =
    Consumed.`with`(serdeForVulcan[K], serdeForVulcan[V])

  def groupedForVulcan[K: VCodec, V: VCodec]: Grouped[K, V] =
    Grouped.`with`(serdeForVulcan[K], serdeForVulcan[V])

}

class VulcanSerdeCreation[T: VCodec] extends Serde[T] {
  override def deserializer(): Deserializer[T] =
    new Deserializer[T] {
      override def deserialize(topic: String, data: Array[Byte]): T = {
        val codec = implicitly[VCodec[T]]
        codec.schema.flatMap(schema => VCodec.fromBinary(data, schema)).fold(error => throw error.throwable, identity)
      }
    }
  override def serializer(): Serializer[T] =
    new Serializer[T] {
      override def serialize(topic: String, data: T): Array[Byte] =
        VCodec.toBinary(data).fold(error => throw error.throwable, identity)
    }
}
