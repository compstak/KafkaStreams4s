package compstak.kafkastreams4s

import compstak.kafkastreams4s.vulcan.VulcanCodec
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed

package object vulcan {
  type VulcanTable[K, V] = STable[VulcanCodec, K, V]

  object VulcanTable {

    def fromKTable[K : VulcanCodec, V : VulcanCodec](ktable: KTable[K, V]): VulcanTable[K, V] =
      new VulcanTable[K, V](ktable)

    def apply[K : VulcanCodec, V : VulcanCodec](sb: StreamsBuilder,topicName: String): VulcanTable[K, V] =
      fromKTable(sb.table(topicName, VulcanSerdes.consumedForVulcan[K, V]))

  }
}
