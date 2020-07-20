package compstak.kafkastreams4s

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.common.header.Headers
import compstak.kafkastreams4s.implicits._
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes
import java.{util => ju}

object SerdeHelpers {

  def emapDeserializer[A, B](sa: Deserializer[A])(f: A => Either[String, B]): Deserializer[B] =
    functorDeserializer.map(sa)(a =>
      f(a) match {
        case Left(error) => throw new RuntimeException(s"Could not deserialize: $error")
        case Right(b) => b
      }
    )

  def emap[A, B](sa: Serde[A])(f: A => Either[String, B])(g: B => A): Serde[B] = {
    val serializerB = contravariantSerializer.contramap(sa.serializer)(g)
    val deserializerB = emapDeserializer(sa.deserializer)(f)

    Serdes.serdeFrom(serializerB, deserializerB)
  }

  def producedForCodec[C[_]: Codec, K: C, V: C]: Produced[K, V] =
    Produced.`with`[K, V](Codec[C].serde[K], Codec[C].serde[V])

  def materializedForCodec[C[_]: Codec, K: C, V: C]: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    Materialized.`with`(Codec[C].serde[K], Codec[C].serde[V])

  def consumedForCodec[C[_]: Codec, K: C, V: C]: Consumed[K, V] =
    Consumed.`with`(Codec[C].serde[K], Codec[C].serde[V])

  def groupedForCodec[C[_]: Codec, K: C, V: C]: Grouped[K, V] =
    Grouped.`with`(Codec[C].serde[K], Codec[C].serde[V])

}
