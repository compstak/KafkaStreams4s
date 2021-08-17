# Introduction

KafkaStreams4s is a library for writing Kafka Streams programs while leveraging the [cats](https://github.com/typelevel/cats) ecosystem.

## Quick example

This example uses the `kafka-streams4s-circe` module, but the code should look similar even if you do not use circe or even JSON.

For this example let's consider a use case where we track movies and movie sales and want to figure out if we can find trends for movie genres.
Modeling this as Scala case classes we write the following:

```scala
import java.util.UUID

case class Movie(title: String, genre: String)
case class Purchase(movieId: UUID, amount: Int, user: String)
```

In order for us to use these case classes with `kafka-streams4s-circe`, we'll need to define `Encoder`s and `Decoder`s for them.

```scala
import io.circe.{Encoder, Decoder}

implicit val movieDecoder: Decoder[Movie] = Decoder.forProduct2("title", "genre")(Movie.apply _)
implicit val movieEncoder: Encoder[Movie] = Encoder.forProduct2("title", "genre")(m => (m.title, m.genre))

implicit val purchaseDecoder: Decoder[Purchase] = Decoder.forProduct3("movieId", "amount", "user")(Purchase.apply _)
implicit val purchaseEncoder: Encoder[Purchase] = 
  Encoder.forProduct3("movieId", "amount", "user")(p => (p.movieId, p.amount, p.user))
```

We will assume here that there exist two kafka topics `example.movies` and `example.purchases` which emit json records.
Next, we will want to create a `CirceTable` for the two topics we want to use for our programs.
A `CirceTable[K, V]` is a wrapper around a `KTable[K, V]` and includes all the necessary codecs so that Kafka Streams is able to serialize and deserialize our records without having to pass around any codecs explicitly. Simply put, we can create a `CirceTable[K, V]` for any `K` and `V` that have a circe `Encoder` and `Decoder` in scope. 


```scala
import compstak.kafkastreams4s.circe.CirceTable
import org.apache.kafka.streams.StreamsBuilder

val sb = new StreamsBuilder

val movies = CirceTable[UUID, Movie](sb, "example.movies")
val purchases = CirceTable[UUID, Purchase](sb, "example.purchases")
```

Now, what we'll want to do is join the two topics, filter out some genres and accumulate the results into a `Map` of genre to number of purchases.

```scala
val withoutOther: CirceTable[UUID, Movie] = movies.filter(_.genre != "Other")
val pairs: CirceTable[UUID, (Purchase, Movie)] = 
  purchases.join(withoutOther)(purchase => purchase.movieId)((purchase, movie) => (purchase, movie))

val result: CirceTable[String, Int] =
  pairs.scanWith { case (id, (purchase, movie)) => (movie.genre, purchase.amount) }(_ + _)

```

Here we first filter out all the movies that are tagged with the `"Other"` genre.
Then we join the movies and purchases topics and lastly we use the `scanWith` operation to select a new KV-pair and then pass a function to aggregate the result.
Now, all that's left is to direct the result into an output topic `example.output` and run the program

```scala
import scala.concurrent.ExecutionContext
import cats._, cats.implicits._
import cats.effect._, cats.effect.implicits._
import compstak.kafkastreams4s.Platform
import org.apache.kafka.streams.Topology
import java.util.Properties
import java.time.Duration

val props = new Properties //in real code add the desired configuration to this object.

val topology: IO[Topology] = result.to[IO]("example.output") >> IO(sb.build())

val main: IO[Unit] = 
  topology.flatMap(topo => Platform.run[IO](topo, props, Duration.ofSeconds(2))).void
```

`compstak.kafkastreams4s.Platform` gives us a function `run` to run Kafka Streams programs and takes a topology, java properties and a timeout after which the stream threads will be shut off. 


## Testing your topologies

KafkaStreams4s comes with a testing module that allows us to test our kafka streams programs without even spinning up a kafka cluster.
To start using it include the `kafka-streams4s-testing` module in your `build.sbt`.
It's built upon `kafka-streams-test-utils` and should give us a good amount of confidence in our streams logic.

First we will create a test driver using our topology defined earlier:

```scala
import cats.effect.Resource
import compstak.kafkastreams4s.testing.KafkaStreamsTestRunner
import org.apache.kafka.streams.TopologyTestDriver

val driver: Resource[IO, TopologyTestDriver] = 
  Resource.eval(topology).flatMap(KafkaStreamsTestRunner.testDriverResource[IO])

```

Then we'll setup some inputs for our two topics:

```scala
import compstak.kafkastreams4s.circe.CirceCodec

val testMovies = List(
  UUID.fromString("150ac164-a4dd-4809-9e2f-fc092edb9d1d") -> Movie("The Godfather", "Crime"),
  UUID.fromString("1312c871-dd07-43a7-ae7b-3a74e0c9ce6d") -> Movie("Schindler's List", "Drama"),
  UUID.fromString("b6168e9a-a871-4712-a15b-0261edc7c9d2") -> Movie("Being John Malkovich", "Other")
)

val testPurchases = List(
  UUID.fromString("faad4c27-39d6-41df-9d97-102dc5b0bc93") -> Purchase(UUID.fromString("b6168e9a-a871-4712-a15b-0261edc7c9d2"), 2, "JohnDoe42"),
  UUID.fromString("310a790e-7df4-4cbb-91f8-42bcf998b9e5") -> Purchase(UUID.fromString("150ac164-a4dd-4809-9e2f-fc092edb9d1d"), 1, "MarkB98"),
  UUID.fromString("5d00d33e-82c4-418e-b776-46b16bd13508") -> Purchase(UUID.fromString("1312c871-dd07-43a7-ae7b-3a74e0c9ce6d"), 1, "JaneDoe"),
  UUID.fromString("0e36dd2f-e3db-4b1e-af45-52f8bc27d65f") -> Purchase(UUID.fromString("150ac164-a4dd-4809-9e2f-fc092edb9d1d"), 3, "NinaD14"),
)

def pipeIn(driver: TopologyTestDriver): IO[Unit] =
  KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "example.movies", testMovies: _*) >>
  KafkaStreamsTestRunner.inputTestTable[IO, CirceCodec](driver, "example.purchases", testPurchases: _*)
  
```


Next, we'll get out the values from the output topic using a different function from the `KafkaStreamsTestRunner`.

```scala
import cats.effect.unsafe.implicits.global

def pipeOut(driver: TopologyTestDriver): IO[Map[String, Int]] =
  KafkaStreamsTestRunner.outputTestTable[IO, CirceCodec, String, Int](driver, "example.output")

driver.use(d => pipeIn(d) >> pipeOut(d)).unsafeRunSync
// res0: Map[String, Int] = Map("Drama" -> 1, "Crime" -> 4)
```