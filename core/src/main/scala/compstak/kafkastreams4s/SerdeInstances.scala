package compstak.kafkastreams4s

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.common.header.Headers
import cats.{Contravariant, Functor, Invariant}
import java.{util => ju}

trait SerdeInstances {

  implicit val functorDeserializer: Functor[Deserializer] = new Functor[Deserializer] {
    def map[A, B](fa: Deserializer[A])(f: A => B): Deserializer[B] =
      new Deserializer[B] {
        def deserialize(topic: String, data: Array[Byte]): B = f(fa.deserialize(topic, data))
        override def close(): Unit = fa.close()
        override def configure(configs: ju.Map[String, _], isKey: Boolean): Unit =
          fa.configure(configs, isKey)
        override def deserialize(topic: String, headers: Headers, data: Array[Byte]): B =
          f(fa.deserialize(topic, headers, data))
      }

  }

  implicit val contravariantSerializer: Contravariant[Serializer] = new Contravariant[Serializer] {
    def contramap[A, B](fa: Serializer[A])(f: B => A): Serializer[B] =
      new Serializer[B] {
        def serialize(topic: String, data: B): Array[Byte] = fa.serialize(topic, f(data))
        override def close(): Unit = fa.close()
        override def configure(configs: ju.Map[String, _], isKey: Boolean): Unit = fa.configure(configs, isKey)
        override def serialize(topic: String, headers: Headers, data: B): Array[Byte] =
          fa.serialize(topic, headers, f(data))
      }
  }

  implicit val invariantSerde: Invariant[Serde] = new Invariant[Serde] {
    def imap[A, B](fa: Serde[A])(f: A => B)(g: B => A): Serde[B] =
      Serdes.serdeFrom(contravariantSerializer.contramap(fa.serializer)(g), functorDeserializer.map(fa.deserializer)(f))
  }
}
