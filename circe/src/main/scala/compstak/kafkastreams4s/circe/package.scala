package compstak.kafkastreams4s

import compstak.kafkastreams4s.circe.CirceCodec
import io.circe.Decoder
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.StreamsBuilder
import io.circe.Encoder

package object circe {
  type CirceTable[K, V] = STable[CirceCodec, K, V]

  object CirceTable {

    def fromKTable[K: Encoder: Decoder, V: Encoder: Decoder](ktable: KTable[K, V]): CirceTable[K, V] =
      new CirceTable[K, V](ktable)

    def apply[K: Encoder: Decoder, V: Encoder: Decoder](sb: StreamsBuilder, topicName: String): CirceTable[K, V] =
      fromKTable(sb.table(topicName, CirceSerdes.consumedForCirce[K, V]))

  }
}
