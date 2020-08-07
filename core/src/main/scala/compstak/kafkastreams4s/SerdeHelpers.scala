package compstak.kafkastreams4s

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
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

  /**
   * Given a Codec C[_] and a value A: C, create a Serde[Option[A]] that will serialize and deserialize `None` as `null`.
   */
  def serdeForNull[C[_]: Codec, V2: C]: Serde[Option[V2]] = new Serde[Option[V2]] {
    def serializer(): Serializer[Option[V2]] = new Serializer[Option[V2]] {
      def serialize(topic: String, v: Option[V2]): Array[Byte] = v match {
        case None => null
        case Some(v2) => Codec[C].serde[V2].serializer.serialize(topic, v2)
      }
    }

    def deserializer(): Deserializer[Option[V2]] = new Deserializer[Option[V2]] {
      def deserialize(topic: String, bytes: Array[Byte]): Option[V2] =
        if (bytes == null)
          None
        else Some(Codec[C].serde[V2].deserializer.deserialize(topic, bytes))
    }
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
