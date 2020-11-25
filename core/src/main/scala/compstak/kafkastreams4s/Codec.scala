package compstak.kafkastreams4s

import org.apache.kafka.common.serialization.Serde

/**
 * A typeclass for codecs that can serialize and deserialize any `A` in Kafka using a `Serde`s
 */
trait Codec[C[_]] {

  // def serialize
  // def deserialize

  def serde[A: C]: Serde[A]
  def optionSerde[A: C]: C[Option[A]]
}

object Codec {
  def apply[C[_]: Codec]: Codec[C] = implicitly

  def codecForOption[C[_]: Codec, A: C]: C[Option[A]] =
    apply[C].optionSerde[A]
}
