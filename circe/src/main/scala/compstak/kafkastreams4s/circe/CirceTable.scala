package compstak.kafkastreams4s.circe

import io.circe.Decoder
import io.circe.Encoder
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.StreamsBuilder
import compstak.kafkastreams4s.circe.CirceSerdes._
import org.apache.kafka.streams.kstream.ValueMapper
import cats.effect.Sync
import org.apache.kafka.streams.kstream.ValueMapperWithKey
import org.apache.kafka.streams.KeyValue

class CirceTable[K: Encoder: Decoder, V: Encoder: Decoder](val toKTable: KTable[K, V]) {
  import CirceTable.fromKTable

  def to[F[_]: Sync](outputTopicName: String): F[Unit] =
    Sync[F].delay(
      toKTable.toStream.to(
        outputTopicName,
        producedForCirce[K, V]
      )
    )

  def keyBy[K2: Encoder: Decoder](f: V => K2): CirceTable[K2, V] =
    fromKTable(
      toKTable
        .groupBy((k: K, v: V) => KeyValue.pair(f(v), v), groupedForCirce[K2, V])
        .aggregate[Option[V]](
          () => Option.empty[V],
          (k: K2, v: V, agg: Option[V]) => Option(v),
          (k: K2, v: V, agg: Option[V]) => agg,
          materializedForCirce[K2, Option[V]]
        )
    ).flattenOption

  def reKey[K2: Encoder: Decoder](f: K => K2): CirceTable[K2, V] =
    fromKTable(
      toKTable
        .groupBy((k: K, v: V) => KeyValue.pair(f(k), v), groupedForCirce[K2, V])
        .aggregate[Option[V]](
          () => Option.empty[V],
          (k: K2, v: V, agg: Option[V]) => Option(v),
          (k: K2, v: V, agg: Option[V]) => agg,
          materializedForCirce[K2, Option[V]]
        )
    ).flattenOption

  def transform[K2: Encoder: Decoder, V2: Encoder: Decoder](f: (K, V) => (K2, V2)): CirceTable[K2, V2] =
    fromKTable(
      toKTable
        .groupBy({ (k: K, v: V) =>
          val (k2, v2) = f(k, v)
          KeyValue.pair(k2, v2)
        }, groupedForCirce[K2, V2])
        .aggregate[Option[V2]](
          () => Option.empty[V2],
          (k: K2, v: V2, agg: Option[V2]) => Option(v),
          (k: K2, v: V2, agg: Option[V2]) => agg,
          materializedForCirce[K2, Option[V2]]
        )
    ).flattenOption

  def join[K2, V2, Z: Encoder: Decoder](
    other: CirceTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): CirceTable[K, Z] =
    fromKTable(toKTable.join(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2), materializedForCirce[K, Z]))

  def joinOption[K2, V2, Z: Encoder: Decoder](
    other: CirceTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Option[Z])(implicit ev: Null <:< Z): CirceTable[K, Z] =
    fromKTable(
      toKTable.join(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2).orNull, materializedForCirce[K, Z])
    )

  def leftJoin[K2, V2, Z: Encoder: Decoder](
    other: CirceTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Z): CirceTable[K, Z] =
    fromKTable(
      toKTable.leftJoin(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2), materializedForCirce[K, Z])
    )

  def leftJoinOption[K2, V2, Z: Encoder: Decoder](
    other: CirceTable[K2, V2]
  )(f: V => K2)(g: (V, V2) => Option[Z])(implicit ev: Null <:< Z): CirceTable[K, Z] =
    fromKTable(
      toKTable.leftJoin(other.toKTable, (v: V) => f(v), (v: V, v2: V2) => g(v, v2).orNull, materializedForCirce[K, Z])
    )

  def scan[V2: Encoder: Decoder](z: => V2)(f: (V2, V) => V2): CirceTable[K, V2] =
    fromKTable(
      toKTable
        .groupBy((k: K, v: V) => KeyValue.pair(k, v), groupedForCirce[K, V])
        .aggregate[V2](
          () => z,
          (k: K, v: V, agg: V2) => f(agg, v),
          (k: K, v: V, agg: V2) => agg,
          materializedForCirce[K, V2]
        )
    )

  def scan1(f: (V, V) => V): CirceTable[K, V] =
    fromKTable(
      toKTable
        .groupBy((k: K, v: V) => KeyValue.pair(k, v), groupedForCirce[K, V])
        .aggregate[Option[V]](
          () => None,
          (k: K, v: V, agg: Option[V]) => Some(agg.fold(v)(acc => f(acc, v))),
          (k: K, v: V, agg: Option[V]) => agg,
          materializedForCirce[K, Option[V]]
        )
    ).flattenOption

  def map[V2: Encoder: Decoder](f: V => V2): CirceTable[K, V2] =
    fromKTable(toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, V2], materializedForCirce[K, V2]))

  def mapWithKey[V2: Encoder: Decoder](f: (K, V) => V2): CirceTable[K, V2] =
    fromKTable(
      toKTable.mapValues(
        ((k: K, v: V) => f(k, v)): ValueMapperWithKey[K, V, V2],
        materializedForCirce[K, V2]
      )
    )

  def mapFilter[V2: Encoder: Decoder](f: V => Option[V2]): CirceTable[K, V2] = {
    val mapped: KTable[K, Option[V2]] =
      toKTable.mapValues(((v: V) => f(v)): ValueMapper[V, Option[V2]], materializedForCirce[K, Option[V2]])
    fromKTable(
      mapped
        .filter((k, o) => o.isDefined)
        .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCirce[K, V2])
    )
  }

  def mapFilterWithKey[V2: Encoder: Decoder](f: (K, V) => Option[V2]): CirceTable[K, V2] = {
    val mapped: KTable[K, Option[V2]] =
      toKTable.mapValues(
        ((k: K, v: V) => f(k, v)): ValueMapperWithKey[K, V, Option[V2]],
        materializedForCirce[K, Option[V2]]
      )
    fromKTable(
      mapped
        .filter((k, o) => o.isDefined)
        .mapValues(((ov: Option[V2]) => ov.get): ValueMapper[Option[V2], V2], materializedForCirce[K, V2])
    )
  }

  def flattenOption[V2: Encoder: Decoder](implicit ev: V =:= Option[V2]): CirceTable[K, V2] =
    fromKTable(
      toKTable
        .filter((k, v) => ev(v).isDefined)
        .mapValues(((v: V) => ev(v).get): ValueMapper[V, V2], materializedForCirce[K, V2])
    )

  def collect[V2: Encoder: Decoder](f: PartialFunction[V, V2]): CirceTable[K, V2] =
    mapFilter(f.lift)
}

object CirceTable {

  def fromKTable[K: Encoder: Decoder, V: Encoder: Decoder](ktable: KTable[K, V]): CirceTable[K, V] =
    new CirceTable[K, V](ktable)

  def apply[K: Encoder: Decoder, V: Encoder: Decoder](sb: StreamsBuilder, topicName: String): CirceTable[K, V] =
    fromKTable(sb.table(topicName, consumedForCirce[K, V]))

  /**
   * Like `CirceTable.apply`, but filters out `null` values.
   */
  def withLogCompaction[K: Encoder: Decoder, V: Encoder: Decoder](
    sb: StreamsBuilder,
    topicName: String
  ): CirceTable[K, V] =
    CirceTable[K, Option[V]](sb, topicName).flattenOption
}
