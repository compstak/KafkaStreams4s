package compstak.kafkastreams4s

import java.sql.Timestamp

import _root_.shapeless._, labelled._, record._

import shapeless._

import compstak.circe.debezium.{DebeziumFieldSchema, DebeziumSchemaPrimitive}
import compstak.kafkastreams4s.debezium.{DebeziumCompositeType, DebeziumPrimitiveType}

class ShapelessTests extends munit.FunSuite {
  implicit val tsDP = new DebeziumPrimitiveType[Timestamp] {
    def debeziumType: DebeziumSchemaPrimitive = DebeziumSchemaPrimitive.Int64
  }

  type CompProposal =
    Record.`Symbol("id") -> Long, Symbol("proposalsIdx") -> Option[Long], Symbol("compBatchId") -> Option[Long], Symbol("dateCreated") -> Timestamp, Symbol("dateUpdated") -> Timestamp, Symbol("marketId") -> Option[Long], Symbol("validated") -> Boolean, Symbol("compVersionId") -> Option[Long], Symbol("verifiedBy") -> Option[String], Symbol("monthly") -> Option[Boolean], Symbol("rollbackReason") -> Option[String], Symbol("stashed") -> Boolean, Symbol("stashReason") -> Option[String]`.T

  test("Get the schema for an extensible record") {
    val dbct = implicitly[DebeziumCompositeType[CompProposal]]
    val schema = dbct.schema
    assertEquals(
      schema,
      List(
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Int64,
          optional = false,
          field = "id"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Int64,
          optional = true,
          field = "proposalsIdx"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Int64,
          optional = true,
          field = "compBatchId"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Int64,
          optional = false,
          field = "dateCreated"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Int64,
          optional = false,
          field = "dateUpdated"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Int64,
          optional = true,
          field = "marketId"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Boolean,
          optional = false,
          field = "validated"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Int64,
          optional = true,
          field = "compVersionId"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.String,
          optional = true,
          field = "verifiedBy"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Boolean,
          optional = true,
          field = "monthly"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.String,
          optional = true,
          field = "rollbackReason"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.Boolean,
          optional = false,
          field = "stashed"
        ),
        DebeziumFieldSchema(
          `type` = DebeziumSchemaPrimitive.String,
          optional = true,
          field = "stashReason"
        )
      )
    )
  }
}
