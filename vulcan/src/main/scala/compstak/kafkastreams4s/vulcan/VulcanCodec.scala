package compstak.kafkastreams4s.vulcan

import org.apache.kafka.common.serialization.Serde
import compstak.kafkastreams4s.Codec
import vulcan.{Codec => VCodec}

trait VulcanCodec[A] {
  implicit def vcodec: VCodec[A]

  def serde: Serde[A] = VulcanSerdes.serdeForVulcan[A]
}

object VulcanCodec {
  def apply[A: VulcanCodec]: VulcanCodec[A] = implicitly

  implicit val cVulcanCodec: Codec[VulcanCodec] =
    new Codec[VulcanCodec] {
      def serde[A: VulcanCodec]: Serde[A] = apply[A].serde
      def optionSerde[A: VulcanCodec]: VulcanCodec[Option[A]] =
        new VulcanCodec[Option[A]] {
          implicit def vcodec: VCodec[Option[A]] = VCodec.option(apply[A].vcodec)
        }
    }

  implicit def vulcanCodecFromVCodec[A](implicit V: VCodec[A]): VulcanCodec[A] =
    new VulcanCodec[A] {
      implicit def vcodec: VCodec[A] = V
    }

}
