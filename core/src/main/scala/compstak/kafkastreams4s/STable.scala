package compstak.kafkastreams4s

import cats.effect.Sync
import cats.data.Ior
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._
import SerdeHelpers._
import cats.kernel.Monoid

/**
 * A Kafka Streams KTable wrapper that abstracts over the codecs used in KTable operations.
 */
class STable[C[_]: Codec, K: C, V: C](val toKTable: KTable[K, V]) {
  import STable.{fromKTable, optionCodec}

  /**
   * Materialize this STable to a topic using the provided topic name and the given codec.
   */
  def to[F[_]: Sync](outputTopicName: String): F[Unit] =
    Sync[F].delay(
      toKTable.toStream.to(
        outputTopicName,
        producedForCodec[C, K, V]
      )
    )

  /**
   * Like `to`, but will render `None` as `null` in the output topic.
   */
  def toRenderOption[F[_]: Sync, V2: C](outputTopicName: String)(implicit ev: V =:= Option[V2]): F[Unit] =
    Sync[F].delay(
      toKTable
        .toStream()
        .to(
          outputTopicName,
          Produced.`with`(
            Codec[C].serde[K],
            serdeForNull[C, V2].asInstanceOf[Serde[V]]
          )
        )
    )

  /**
   * Like `to`, but will remove any `null`s that may arise from log compaction or using any of the `filter` methods.
   */
  def toRemoveNulls[F[_]: Sync](outputTopicName: String): F[Unit] =
    Sync[F].delay(
      toKTable.toStream
        .filter((k, v) => v != null)
        .to(
          outputTopicName,
          producedForCodec[C, K, V]
        )
    )

  def keyBy[K2: C](f: V => K2): STable[C, K2, V] =
    fromKTable(
      toKTable
        .groupBy(
          ((k: K, v: V) => KeyValue.pair(f(v), v)): KeyValueMapper[K, V, KeyValue[K2, V]],
          groupedForCodec[C, K2, V]
        )
        .reduce((acc: V, cur: V) => cur, (acc: V, old: V) => acc)
    )

  def reKey[K2: C](f: K => K2): STable[C, K2, V] =
    fromKTable(
      toKTable
        .groupBy(
          ((k: K, v: V) => KeyValue.pair(f(k), v)): KeyValueMapper[K, V, KeyValue[K2, V]],
          groupedForCodec[C, K2, V]
        )
        .reduce((acc: V, cur: V) => cur, (acc: V, old: V) => acc)
    )

  def transform[K2: C, V2: C](f: (K, V) => (K2, V2)): STable[C, K2, V2] =
    fromKTable(
      toKTable
        .groupBy(
          { (k: K, v: V) =>
            val (k2, v2) = f(k, v)
            KeyValue.pair(k2, v2)
          }: KeyValueMapper[K, V, KeyValue[K2, V2]],
          groupedForCodec[C, K2, V2]
        )
        .reduce((acc: V2, cur: V2) => cur, (acc: V2, old: V2) => acc)
    )

  def join[K2, V2, Z: C](
    other: STable[C, K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): STable[C, K, Z] =
    fromKTable(toKTable.join(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2), materializedForCodec[C, K, Z]))

  def joinOption[K2, V2, Z: C](
    other: STable[C, K2, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z)(implicit ev: Null <:< K2): STable[C, K, Z] =
    fromKTable(
      toKTable.join(other.toKTable, (v: V) => f(v).orNull, (v: V, v2: V2) => g(v, v2), materializedForCodec[C, K, Z])
    )

  def keyJoin[V2, Z: C](other: STable[C, K, V2])(f: (V, V2) => Z): STable[C, K, Z] =
    fromKTable(
      toKTable.join(other.toKTable, ((v: V, v2: V2) => f(v, v2)): ValueJoiner[V, V2, Z], materializedForCodec[C, K, Z])
    )

  def keyLeftJoin[V2, Z: C](other: STable[C, K, V2])(f: (V, Option[V2]) => Z): STable[C, K, Z] =
    fromKTable(
      toKTable.leftJoin(
        other.toKTable,
        ((v: V, v2: V2) => f(v, Option(v2))): ValueJoiner[V, V2, Z],
        materializedForCodec[C, K, Z]
      )
    )

  def keyOuterJoin[V2: C, Z: C](other: STable[C, K, V2])(f: Ior[V, V2] => Z): STable[C, K, Z] =
    fromKTable(
      toKTable.outerJoin(
        other.toKTable,
        ((v: V, v2: V2) => f(toIor(v, v2))): ValueJoiner[V, V2, Z],
        materializedForCodec[C, K, Z]
      )
    )

  def leftJoin[K2, V2, Z: C](
    other: STable[C, K2, V2]
  )(f: V => K2)(g: (V, Option[V2]) => Z): STable[C, K, Z] =
    fromKTable(
      toKTable
        .leftJoin(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, Option(v2)), materializedForCodec[C, K, Z])
    )

  def leftJoinOption[K2, V2, Z: C](
    other: STable[C, K2, V2]
  )(f: V => Option[K2])(g: (V, Option[V2]) => Z)(implicit ev: Null <:< K2): STable[C, K, Z] =
    fromKTable(
      toKTable
        .leftJoin(
          other.toKTable,
          (v: V) => f(v).orNull,
          (v: V, v2: V2) => g(v, Option(v2)),
          materializedForCodec[C, K, Z]
        )
    )

  def scan[V2: C](z: => V2)(f: (V2, V) => V2): STable[C, K, V2] =
    fromKTable(
      toKTable
        .groupBy(((k: K, v: V) => KeyValue.pair(k, v)): KeyValueMapper[K, V, KeyValue[K, V]], groupedForCodec[C, K, V])
        .aggregate[V2](
          (() => z): Initializer[V2],
          ((k: K, v: V, agg: V2) => f(agg, v)): Aggregator[K, V, V2],
          ((k: K, v: V, agg: V2) => agg): Aggregator[K, V, V2],
          materializedForCodec[C, K, V2]
        )
    )

  def scan1(f: (V, V) => V): STable[C, K, V] =
    fromKTable(
      toKTable
        .groupBy(((k: K, v: V) => KeyValue.pair(k, v)): KeyValueMapper[K, V, KeyValue[K, V]], groupedForCodec[C, K, V])
        .reduce((acc: V, cur: V) => f(acc, cur), (acc: V, old: V) => acc)
    )

  def scanWith[K2: C, V2: C](f: (K, V) => (K2, V2))(g: (V2, V2) => V2): STable[C, K2, V2] =
    fromKTable(
      toKTable
        .groupBy(
          { (k: K, v: V) =>
            val (k2, v2) = f(k, v)
            KeyValue.pair(k2, v2)
          }: KeyValueMapper[K, V, KeyValue[K2, V2]],
          groupedForCodec[C, K2, V2]
        )
        .reduce((acc: V2, cur: V2) => g(acc, cur), (acc: V2, old: V2) => acc)
    )

  def scanMap[V2: C: Monoid](f: V => V2): STable[C, K, V2] =
    scan(Monoid[V2].empty)((v2, v) => Monoid[V2].combine(v2, f(v)))

  def map[V2: C](f: V => V2): STable[C, K, V2] =
    fromKTable(toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, V2], materializedForCodec[C, K, V2]))

  def mapWithKey[V2: C](f: (K, V) => V2): STable[C, K, V2] =
    fromKTable(
      toKTable.mapValues(
        ((k: K, v: V) => f(k, v)): ValueMapperWithKey[K, V, V2],
        materializedForCodec[C, K, V2]
      )
    )

  def mapFilter[V2: C](f: V => Option[V2]): STable[C, K, V2] = {
    val mapped: KTable[K, Option[V2]] =
      toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, Option[V2]], materializedForCodec[C, K, Option[V2]])
    fromKTable(
      mapped
        .filter((k, o) => o.isDefined)
        .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCodec[C, K, V2])
    )
  }

  def mapFilterWithKey[V2: C](f: (K, V) => Option[V2]): STable[C, K, V2] = {
    val mapped: KTable[K, Option[V2]] =
      toKTable.mapValues(
        ((k: K, v: V) => f(k, v)): ValueMapperWithKey[K, V, Option[V2]],
        materializedForCodec[C, K, Option[V2]]
      )
    fromKTable(
      mapped
        .filter((k, o) => o.isDefined)
        .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCodec[C, K, V2])
    )
  }

  def flattenOption[V2: C](implicit ev: V =:= Option[V2]): STable[C, K, V2] =
    fromKTable(
      toKTable
        .filter(((k: K, v: V) => ev(v).isDefined): Predicate[K, V])
        .mapValues(((v: V) => ev(v).get): ValueMapper[V, V2], materializedForCodec[C, K, V2])
    )

  def filter(f: V => Boolean): STable[C, K, V] =
    fromKTable(toKTable.filter(((k: K, v: V) => f(v)): Predicate[K, V]))

  def collect[V2: C](f: PartialFunction[V, V2]): STable[C, K, V2] =
    mapFilter(f.lift)

  def mapCodec[C2[_]: Codec](implicit K: C2[K], V: C2[V]): STable[C2, K, V] = {
    implicit def c2Option[A: C2]: C2[Option[A]] = Codec.codecForOption[C2, A]
    fromKTable[C2, K, V](
      toKTable
        .groupBy(KeyValue.pair: KeyValueMapper[K, V, KeyValue[K, V]], groupedForCodec[C2, K, V])
        .reduce((acc: V, cur: V) => cur, (acc: V, old: V) => acc)
    )
  }

  private def toIor[A, B](a: A, b: B): Ior[A, B] =
    if (a != null && b != null) Ior.both(a, b)
    else if (a == null) Ior.right(b)
    else Ior.left(a)
}

object STable {
  implicit def optionCodec[C[_]: Codec, A: C]: C[Option[A]] =
    Codec.codecForOption

  def fromKTable[C[_]: Codec, K: C, V: C](ktable: KTable[K, V]): STable[C, K, V] =
    new STable[C, K, V](ktable)

  def apply[C[_]: Codec, K: C, V: C](sb: StreamsBuilder, topicName: String): STable[C, K, V] =
    fromKTable(sb.table(topicName, consumedForCodec[C, K, V]))
}
