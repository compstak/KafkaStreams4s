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
class STable[HK[_]: Codec, K: HK, HV[_]: Codec, V: HV](val toKTable: KTable[K, V]) {
  import STable.{fromKTable, optionCodec}

  /**
   * Materialize this STable to a topic using the provided topic name and the given codec.
   */
  def to[F[_]: Sync](outputTopicName: String): F[Unit] =
    Sync[F].delay(
      toKTable.toStream.to(
        outputTopicName,
        producedForCodec[HK, K, HV, V]
      )
    )

  /**
   * Like `to`, but will render `None` as `null` in the output topic.
   */
  def toRenderOption[F[_]: Sync, V2: HV](outputTopicName: String)(implicit ev: V =:= Option[V2]): F[Unit] =
    Sync[F].delay(
      toKTable
        .toStream()
        .to(
          outputTopicName,
          Produced.`with`(
            Codec[HK].serde[K],
            serdeForNull[HV, V2].asInstanceOf[Serde[V]] // LIES
            // ev.apply()
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
          producedForCodec[HK, K, HV, V]
        )
    )

  def keyBy[K2: HK](f: V => K2): STable[HK, K2, HV, V] =
    fromKTable(
      toKTable
        .groupBy[K2, V](
          ((k: K, v: V) => KeyValue.pair(f(v), v)): KeyValueMapper[K, V, KeyValue[K2, V]],
          groupedForCodec[HK, K2, HV, V]
        )
        .reduce((acc: V, cur: V) => cur, (acc: V, old: V) => acc)
    )

  def reKey[K2: HK](f: K => K2): STable[HK, K2, HV, V] =
    fromKTable(
      toKTable
        .groupBy(
          ((k: K, v: V) => KeyValue.pair(f(k), v)): KeyValueMapper[K, V, KeyValue[K2, V]],
          groupedForCodec[HK, K2, HV, V]
        )
        .reduce((acc: V, cur: V) => cur, (acc: V, old: V) => acc)
    )

  def transform[K2: HK, V2: HV](f: (K, V) => (K2, V2)): STable[HK, K2, HV, V2] =
    fromKTable(
      toKTable
        .groupBy(
          { (k: K, v: V) =>
            val (k2, v2) = f(k, v)
            KeyValue.pair(k2, v2)
          }: KeyValueMapper[K, V, KeyValue[K2, V2]],
          groupedForCodec[HK, K2, HV, V2]
        )
        .reduce((acc: V2, cur: V2) => cur, (acc: V2, old: V2) => acc)
    )

  def join[K2, V2, Z: HV](
    other: STable[HK, K2, HV, V2]
  )(f: V => K2)(g: (V, V2) => Z): STable[HK, K, HV, Z] =
    fromKTable(
      toKTable.join(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2), materializedForCodec[HK, K, HV, Z])
    )

  def joinOption[K2, V2, Z: HV](
    other: STable[HK, K2, HV, V2]
  )(f: V => Option[K2])(g: (V, V2) => Z)(implicit ev: Null <:< K2): STable[HK, K, HV, Z] =
    fromKTable(
      toKTable.join(
        other.toKTable,
        (v: V) => f(v).orNull,
        (v: V, v2: V2) => g(v, v2),
        materializedForCodec[HK, K, HV, Z]
      )
    )

  def keyJoin[V2, Z: HV](other: STable[HK, K, HV, V2])(f: (V, V2) => Z): STable[HK, K, HV, Z] =
    fromKTable(
      toKTable.join(
        other.toKTable,
        ((v: V, v2: V2) => f(v, v2)): ValueJoiner[V, V2, Z],
        materializedForCodec[HK, K, HV, Z]
      )
    )

  def keyLeftJoin[V2, Z: HV](other: STable[HK, K, HV, V2])(f: (V, Option[V2]) => Z): STable[HK, K, HV, Z] =
    fromKTable(
      toKTable.leftJoin(
        other.toKTable,
        ((v: V, v2: V2) => f(v, Option(v2))): ValueJoiner[V, V2, Z],
        materializedForCodec[HK, K, HV, Z]
      )
    )

  def keyOuterJoin[V2, Z: HV](other: STable[HK, K, HV, V2])(f: Ior[V, V2] => Z): STable[HK, K, HV, Z] =
    fromKTable(
      toKTable.outerJoin(
        other.toKTable,
        ((v: V, v2: V2) => f(toIor(v, v2))): ValueJoiner[V, V2, Z],
        materializedForCodec[HK, K, HV, Z]
      )
    )

  def leftJoin[K2, V2, Z: HV](
    other: STable[HK, K2, HV, V2]
  )(f: V => K2)(g: (V, Option[V2]) => Z): STable[HK, K, HV, Z] =
    fromKTable(
      toKTable
        .leftJoin(
          other.toKTable,
          (v: V) => f(v),
          (v: V, v2: V2) => g(v, Option(v2)),
          materializedForCodec[HK, K, HV, Z]
        )
    )

  def leftJoinOption[K2, V2, Z: HV](
    other: STable[HK, K2, HV, V2]
  )(f: V => Option[K2])(g: (V, Option[V2]) => Z)(implicit ev: Null <:< K2): STable[HK, K, HV, Z] =
    fromKTable(
      toKTable
        .leftJoin(
          other.toKTable,
          (v: V) => f(v).orNull,
          (v: V, v2: V2) => g(v, Option(v2)),
          materializedForCodec[HK, K, HV, Z]
        )
    )

  def scan[V2: HV](z: => V2)(f: (V2, V) => V2): STable[HK, K, HV, V2] =
    fromKTable(
      toKTable
        .groupBy(
          ((k: K, v: V) => KeyValue.pair(k, v)): KeyValueMapper[K, V, KeyValue[K, V]],
          groupedForCodec[HK, K, HV, V]
        )
        .aggregate[V2](
          (() => z): Initializer[V2],
          ((k: K, v: V, agg: V2) => f(agg, v)): Aggregator[K, V, V2],
          ((k: K, v: V, agg: V2) => agg): Aggregator[K, V, V2],
          materializedForCodec[HK, K, HV, V2]
        )
    )

  def scan1(f: (V, V) => V): STable[HK, K, HV, V] =
    fromKTable(
      toKTable
        .groupBy(
          ((k: K, v: V) => KeyValue.pair(k, v)): KeyValueMapper[K, V, KeyValue[K, V]],
          groupedForCodec[HK, K, HV, V]
        )
        .reduce((acc: V, cur: V) => f(acc, cur), (acc: V, old: V) => acc)
    )

  def scanWith[K2: HK, V2: HV](f: (K, V) => (K2, V2))(g: (V2, V2) => V2): STable[HK, K2, HV, V2] =
    fromKTable(
      toKTable
        .groupBy(
          { (k: K, v: V) =>
            val (k2, v2) = f(k, v)
            KeyValue.pair(k2, v2)
          }: KeyValueMapper[K, V, KeyValue[K2, V2]],
          groupedForCodec[HK, K2, HV, V2]
        )
        .reduce((acc: V2, cur: V2) => g(acc, cur), (acc: V2, old: V2) => acc)
    )

  def scanMap[V2: HV: Monoid](f: V => V2): STable[HK, K, HV, V2] =
    scan(Monoid[V2].empty)((v2, v) => Monoid[V2].combine(v2, f(v)))

  def map[V2: HV](f: V => V2): STable[HK, K, HV, V2] =
    fromKTable(toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, V2], materializedForCodec[HK, K, HV, V2]))

  def mapWithKey[V2: HV](f: (K, V) => V2): STable[HK, K, HV, V2] =
    fromKTable(
      toKTable.mapValues(
        ((k: K, v: V) => f(k, v)): ValueMapperWithKey[K, V, V2],
        materializedForCodec[HK, K, HV, V2]
      )
    )

  def mapFilter[V2: HV](f: V => Option[V2]): STable[HK, K, HV, V2] =
    // val mapped: KTable[K, Option[V2]] =
    //   toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, Option[V2]], materializedForCodec[HK, K, HV, Option[V2]])
    // fromKTable(
    //   mapped
    //     .filter((k, o) => o.isDefined)
    //     .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCodec[HK, K, HV, V2])
    // )
    // TODO
    ???

  def mapFilterWithKey[V2: HV](f: (K, V) => Option[V2]): STable[HK, K, HV, V2] =
    // val mapped: KTable[K, Option[V2]] =
    //   toKTable.mapValues(
    //     ((k: K, v: V) => f(k, v)): ValueMapperWithKey[K, V, Option[V2]],
    //     materializedForCodec[HK, K,HV, Option[V2]]
    //   )
    // fromKTable(
    //   mapped
    //     .filter((k, o) => o.isDefined)
    //     .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCodec[HK, K,HV, V2])
    // )
    // TODO
    ???

  def flattenOption[V2: HV](implicit ev: V =:= Option[V2]): STable[HK, K, HV, V2] =
    fromKTable(
      toKTable
        .filter(((k: K, v: V) => ev(v).isDefined): Predicate[K, V])
        .mapValues(((v: V) => ev(v).get): ValueMapper[V, V2], materializedForCodec[HK, K, HV, V2])
    )

  def filter(f: V => Boolean): STable[HK, K, HV, V] =
    fromKTable(toKTable.filter(((k: K, v: V) => f(v)): Predicate[K, V]))

  def collect[V2: HV](f: PartialFunction[V, V2]): STable[HK, K, HV, V2] =
    mapFilter(f.lift)

  def mapCodec[HK2[_]: Codec, HV2[_]: Codec](implicit K: HK2[K], V: HV2[V]): STable[HK2, K, HV2, V] =
    fromKTable[HK2, K, HV2, V](
      toKTable
        .groupBy(KeyValue.pair: KeyValueMapper[K, V, KeyValue[K, V]], groupedForCodec[HK2, K, HV2, V])
        .reduce((acc: V, cur: V) => cur, (acc: V, old: V) => acc)
    )

  private def toIor[A, B](a: A, b: B): Ior[A, B] =
    if (a != null && b != null) Ior.both(a, b)
    else if (a == null) Ior.right(b)
    else Ior.left(a)
}

object STable {
  implicit def optionCodec[C[_]: Codec, A: C]: C[Option[A]] =
    Codec.codecForOption

  def fromKTable[HK[_]: Codec, K: HK, HV[_]: Codec, V: HV](ktable: KTable[K, V]): STable[HK, K, HV, V] =
    new STable[HK, K, HV, V](ktable)

  def apply[HK[_]: Codec, K: HK, HV[_]: Codec, V: HV](sb: StreamsBuilder, topicName: String): STable[HK, K, HV, V] =
    fromKTable(sb.table(topicName, consumedForCodec[HK, K, HV, V]))
}
