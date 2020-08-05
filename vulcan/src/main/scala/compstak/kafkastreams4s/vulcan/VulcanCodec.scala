package compstak.kafkastreams4s.vulcan

import org.apache.kafka.common.serialization.Serde
import compstak.kafkastreams4s.Codec
import vulcan.{Codec => VCodec}

trait VulcanCodec[A] {
  implicit def vcodec: VCodec[A]

  def serde: Serde[A] = CirceSerdes.serdeForCirce[A]
}

object VulcanCodec {
  def apply[A: VulcanCodec]: VulcanCodec[A] = implicitly

  implicit def cVulcanCodec: Codec[VulcanCodec] = new Codec[VulcanCodec] {
    def serde[A: VulcanCodec]: Serde[A] = apply[A].serde
    def optionSerde[A](implicit ev: VulcanCodec[A]): VulcanCodec[Option[A]] = new VulcanCodec[Option[A]] {
      implicit def encoder: Encoder[Option[A]] = Encoder.encodeOption(ev.encoder)
      implicit def decoder: Decoder[Option[A]] = Decoder.decodeOption(ev.decoder)
    }
  }

}
