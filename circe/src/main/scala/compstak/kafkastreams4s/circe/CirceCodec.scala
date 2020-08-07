package compstak.kafkastreams4s.circe

import org.apache.kafka.common.serialization.Serde
import io.circe.{Decoder, Encoder}
import compstak.kafkastreams4s.Codec

trait CirceCodec[A] {
  implicit def encoder: Encoder[A]
  implicit def decoder: Decoder[A]

  def serde: Serde[A] = CirceSerdes.serdeForCirce[A]
}

object CirceCodec {
  def apply[A: CirceCodec]: CirceCodec[A] = implicitly

  implicit def ccCirceCodec: Codec[CirceCodec] =
    new Codec[CirceCodec] {
      def serde[A: CirceCodec]: Serde[A] = apply[A].serde
      def optionSerde[A](implicit ev: CirceCodec[A]): CirceCodec[Option[A]] =
        new CirceCodec[Option[A]] {
          implicit def encoder: Encoder[Option[A]] = Encoder.encodeOption(ev.encoder)
          implicit def decoder: Decoder[Option[A]] = Decoder.decodeOption(ev.decoder)
        }
    }

  implicit def circeCodecFromEncoderDecoder[A](implicit e: Encoder[A], d: Decoder[A]): CirceCodec[A] =
    new CirceCodec[A] {
      def encoder: Encoder[A] = e
      def decoder: Decoder[A] = d
    }
}
