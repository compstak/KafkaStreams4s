package compstak.kafkastreams4s

import org.apache.kafka.common.serialization.Serde

/**
 * A generic codec for serializing and deserializing an `A` in Kafka using a `Serde`
 */
trait Codec[A] {
  def serde: Serde[A]
}

object Codec {
  def apply[A: Codec]: Codec[A] = implicitly

  implicit def codecForOption[C[x] <: Codec[x], A](implicit C: CodecOption[C], A: C[A]): C[Option[A]] =
    C.optionCodec
}
