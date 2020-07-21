package compstak.kafkastream4s.avro4s

import org.apache.kafka.common.serialization.Serde
import com.sksamuel.avro4s.kafka.GenericSerde
import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.Decoder
import shapeless.Generic
import compstak.kafkastreams4s.avro4s.Avro4sSerdes
import compstak.kafkastreams4s.Codec

trait Avro4sCodec[A] {
  def schema: SchemaFor[A]
  def encoder: Encoder[A]
  def decoder: Decoder[A]
  def ev: Null <:< A

  def serde: Serde[A] =
    Avro4sSerdes
      .serdeForAvro4s[AnyRef](
        encoder.asInstanceOf[Encoder[AnyRef]],
        decoder.asInstanceOf[Decoder[AnyRef]],
        schema.asInstanceOf[SchemaFor[AnyRef]]
      )
      .asInstanceOf[Serde[A]]
}

object Avro4sCodec {
  def apply[A: Avro4sCodec]: Avro4sCodec[A] = implicitly

  implicit val avro4sCodecOption: Codec[Avro4sCodec] = new Codec[Avro4sCodec] {
    def serde[A: Avro4sCodec]: Serde[A] = apply[A].serde

    def optionSerde[A: Avro4sCodec]: Avro4sCodec[Option[A]] = new Avro4sCodec[Option[A]] {
      def schema: SchemaFor[Option[A]] = SchemaFor.optionSchemaFor[A](apply[A].schema)
      def encoder: Encoder[Option[A]] = Encoder.optionEncoder[A](apply[A].encoder)
      def decoder: Decoder[Option[A]] = Decoder.optionDecoder[A](apply[A].decoder)

      def ev: Null <:< Option[A] = implicitly
    }
  }

  implicit def avro4sCodecFromSchemaEncoderDecoder[A](
    implicit ev0: Null <:< A,
    S: SchemaFor[A],
    E: Encoder[A],
    D: Decoder[A]
  ): Avro4sCodec[A] = new Avro4sCodec[A] {
    implicit def schema: SchemaFor[A] = S
    implicit def encoder: Encoder[A] = E
    implicit def decoder: Decoder[A] = D

    def ev: Null <:< A = ev0

  }

}
