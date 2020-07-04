package compstak.kafkastreams4s

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.common.header.Headers
import java.{util => ju}
import compstak.kafkastreams4s.implicits._

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
}
