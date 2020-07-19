package compstak.kafkastreams4s

/**
 * A typeclass that allows you to extend any `Codec[A]` to `Codec[Option[A]]`
 */
trait CodecOption[C[_]] {
  def optionCodec[A: C]: C[Option[A]]
}

object CodecOption {
  def apply[C[_]: CodecOption]: CodecOption[C] = implicitly
}
