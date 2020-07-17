package compstak.kafkastreams4s

trait CodecOption[C[_]] {
  def optionCodec[A: C]: C[Option[A]]
}

object CodecOption {
  def apply[C[_]: CodecOption]: CodecOption[C] = implicitly
}
