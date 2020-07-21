package compstak.kafkastreams4s.avro4s

import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.Decoder
import com.sksamuel.avro4s.SchemaFor
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes
import com.sksamuel.avro4s.kafka.GenericSerde

object Avro4sSerdes {

  def serdeForAvro4s[A >: Null: Encoder: Decoder: SchemaFor]: Serde[A] = new GenericSerde[A]

  def producedForAvro4s[K >: Null: Encoder: Decoder: SchemaFor, V >: Null: Encoder: Decoder: SchemaFor]
    : Produced[K, V] =
    Produced.`with`(serdeForAvro4s[K], serdeForAvro4s[V])

  def materializedForAvro4s[K >: Null: Encoder: Decoder: SchemaFor, V >: Null: Encoder: Decoder: SchemaFor]
    : Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    Materialized
      .`with`(serdeForAvro4s[K], serdeForAvro4s[V])

  def consumedForAvro4s[K >: Null: Encoder: Decoder: SchemaFor, V >: Null: Encoder: Decoder: SchemaFor]
    : Consumed[K, V] =
    Consumed.`with`(serdeForAvro4s[K], serdeForAvro4s[V])

  def groupedForAvro4s[K >: Null: Encoder: Decoder: SchemaFor, V >: Null: Encoder: Decoder: SchemaFor]: Grouped[K, V] =
    Grouped.`with`(serdeForAvro4s[K], serdeForAvro4s[V])
}
