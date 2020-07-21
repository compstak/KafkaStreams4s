package compstak.kafkastreams4s

import compstak.kafkastream4s.avro4s.Avro4sCodec
import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.Decoder
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed

package object avro4s {
  type Avro4sTable[K, V] = STable[Avro4sCodec, K, V]

  object Avro4sTable {

    def fromKTable[K >: Null: SchemaFor: Encoder: Decoder, V >: Null: SchemaFor: Encoder: Decoder](
      ktable: KTable[K, V]
    ): Avro4sTable[K, V] =
      new Avro4sTable[K, V](ktable)

    def apply[K >: Null: SchemaFor: Encoder: Decoder, V >: Null: SchemaFor: Encoder: Decoder](
      sb: StreamsBuilder,
      topicName: String
    ): Avro4sTable[K, V] =
      fromKTable(sb.table(topicName, Avro4sSerdes.consumedForAvro4s[K, V]))

    /**
     * Like `Avro4sTable.apply`, but filters out `null` values.
     */
    def withLogCompaction[K >: Null: SchemaFor: Encoder: Decoder, V >: Null: SchemaFor: Encoder: Decoder](
      sb: StreamsBuilder,
      topicName: String
    ): Avro4sTable[K, V] =
      Avro4sTable[K, Option[V]](sb, topicName).flattenOption
  }
}
