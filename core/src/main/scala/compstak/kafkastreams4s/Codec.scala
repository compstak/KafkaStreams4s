package compstak.kafkastreams4s

import org.apache.kafka.common.serialization.Serde

trait Codec[A] {
  def serde: Serde[A]
}

object Codec {
  def apply[A: Codec]: Codec[A] = implicitly

  implicit def codecForOption[C[x] <: Codec[x], A](implicit C: CodecOption[C], A: C[A]): C[Option[A]] =
    C.optionCodec
}
