package com.twosigma.flint.timeseries.experiment

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{LeftSubtractableSummarizer => LeftSubtractableSummarizerFunction}
import com.twosigma.flint.rdd.{KeyPartitioningType, OrderedRDD}
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{BaseSummarizerFactory, ColumnList, LeftSubtractableSummarizer}
import com.twosigma.flint.timeseries.{Summarizers, TimeSeriesRDD, TimeSeriesSuite, Windows}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema => ExternalRow}
import org.apache.spark.sql.types._

object SelectiveAccumulateSummarizerSpec {
  def trace(msg: String): Unit = {
    println(s"[Thread-${Thread.currentThread().getId}] $msg")
  }

  trait Event {val time: Long}
  case class SnapshotEvent(time: Long, value: Long) extends Event
  case class DeltaEvent(time: Long, value: Long) extends Event
  case class GenerateEvent(time: Long) extends Event

  case class SummarizerFunction() extends LeftSubtractableSummarizerFunction[Event, (Option[Long], Option[Long], Boolean), Option[Long]] {
    type T = Event
    type U = (Option[Long], Option[Long], Boolean)
    type V = Option[Long]

    override def subtract(u: U, v: T): U = {
      val result = u
      trace(s"subtract($u, $v) => $result")
      result
    }

    /**
      * @return the initial state of this summarizer.
      */
    override def zero(): U = {
      val result = (None, None, false)
      trace(s"zero() => $result")
      result
    }

    /**
      * Update a state with a given value. The original u can be changed after [[add()]].
      *
      * @param u The prior state.
      * @param t A value expected to use for updating the prior state.
      * @return updated state
      */
    override def add(u: U, t: T): U = {
      val result = (u, t) match {
        case ((_, None, _), SnapshotEvent(time, value)) => (Some(time), Some(value), false)
        case ((_, None, _), DeltaEvent(time, value)) => (Some(time), None, false)
        case ((_, None, _), GenerateEvent(time)) => (Some(time), None, true)
        case ((_, Some(state), _), SnapshotEvent(time, value)) => if (state == value) (Some(time), Some(state), false) else throw new RuntimeException("snapshot doesn't match")
        case ((_, Some(state), _), DeltaEvent(time, value)) => (Some(time), Some(state + value), false)
        case ((_, Some(state), _), GenerateEvent(time)) => (Some(time), Some(state), true)
        case _ => throw new RuntimeException("should not happen")
      }
      trace(s"add($u, $t) => $result")
      result
    }

    /**
      * Merge two summarizer states. The original u1 and u2 can be changed after [[merge()]]
      *
      * For two sequences (a[1], a[2], ..., a[n]), (a'[1], a'[2], ..., a'[m]), and two states u1 and
      * u1 where
      * u1 = 0 + a[1] + ... + a[n]
      * u2 = 0 + a'[1] + ... + a'[m]
      *
      * It must satisfy the following condition.
      * u1 + u2 = 0 + a[1] + ... + a[n] + a'[1] + ... + a'[m]
      *
      * @param u1 A state expected to merge as the left hand side of merge operation.
      * @param u2 A state expected to merge as the right hand side of merge operation.
      * @return the merged state
      */
    override def merge(u1: U, u2: U): U = {
      val result = (u1, u2) match {
        case ((_, None, _), (time, None, generate)) => (time, None, generate)
        case ((_, None, _), (time, Some(uu2), generate)) => (time, Some(uu2), generate)
        case ((_, Some(uu1), _), (time, None, generate)) => (time, None, generate)  // I assume time of u1 will be smaller than u2, is this true?
        case ((_, Some(uu1), _), (time, Some(uu2), generate)) => (time, Some(uu2), generate)  // I assume the time of u1 and u2 are consective, is this true?
      }
      trace(s"merge($u1, $u2) => $result")
      result
    }

    /**
      * Renders a state into the output type. The original u should NOT change after [[render()]]
      *
      * @param u state expected to render
      * @return a rendered value with desired type.
      */
    override def render(u: U): V = {
      val result = u match {
        case (_, _, false) => None
        case (_, state, true) => state
      }
      trace(s"render($u) => $result")
      result
    }
  }

  case class Summarizer(override val inputSchema: StructType,
                        override val prefixOpt: Option[String],
                        override val requiredColumns: ColumnList) extends LeftSubtractableSummarizer {
    override val summarizer = SummarizerFunction()
    override type T = Event
    override type U = (Option[Long], Option[Long], Boolean)
    override type V = Option[Long]

    private val timeIdx = inputSchema.fieldIndex(TimeSeriesRDD.timeColumnName)
    private val snapshotIdx = inputSchema.fieldIndex("snapshot")
    private val deltaIdx = inputSchema.fieldIndex("delta")
    private val generateIdx = inputSchema.fieldIndex("generate")

    override def toT(r: InternalRow): T = {
      val time = r.getLong(timeIdx)
      (r.getBoolean(generateIdx), r.isNullAt(snapshotIdx), r.isNullAt(deltaIdx)) match {
        case (false, true, false) => DeltaEvent(time, r.getLong(deltaIdx))
        case (false, false, true) => SnapshotEvent(time, r.getLong(snapshotIdx))
        case (true, true, true) => GenerateEvent(time)
        case x => throw new RuntimeException(s"data is wrong: $x")
      }
    }

    override def fromV(v: V): InternalRow = v match {
      case Some(vv) => InternalRow(vv)
      case None => InternalRow(null)
    }

    override def isValid(r: InternalRow): Boolean = true

    /**
      * The schema of output rows. The output schema will be exactly this `schema` if `alias` is `None`.
      * Otherwise, it will be prepend the alias.
      */
    override val schema: StructType = Schema.of("reconstructed_snapshot" -> LongType)
  }

  case class SummarizerFactory() extends BaseSummarizerFactory(TimeSeriesRDD.timeColumnName, "snapshot", "delta") {
    override def apply(inputSchema: StructType): Summarizer = {
      Summarizer(inputSchema, prefixOpt, requiredColumns)
    }
  }
}



class SelectiveAccumulateSummarizerSpec extends TimeSeriesSuite {

  "Summarizer" should "work on merge with very simple data" in {
    val schema = Schema("time" -> LongType, "snapshot" -> LongType, "delta" -> LongType)
    val defaultNumPartitions = 3

    val data = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, null, 1L), schema)),
      (1010L, new ExternalRow(Array(1010L, null, 1L), schema)),
      (1050L, new ExternalRow(Array(1050L, 8L, null), schema)),
      (1100L, new ExternalRow(Array(1100L, null, 1L), schema)),
      (1200L, new ExternalRow(Array(1200L, null, 1L), schema)),
      (1250L, new ExternalRow(Array(1250L, null, 1L), schema)),
      (1350L, new ExternalRow(Array(1350L, 11L, null), schema)),
      (1550L, new ExternalRow(Array(1550L, null, 1L), schema))
    )

    val ts = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(data, defaultNumPartitions), KeyPartitioningType.Sorted),
      schema
    ).addColumns("generate" -> BooleanType -> {_ => false})
    ts.cache()
    ts.count()
    ts.toDF.show()

    val clockSchema = Schema("time" -> LongType)
    val clockData = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L), clockSchema)),
      (1100L, new ExternalRow(Array(1100L), clockSchema)),
      (1200L, new ExternalRow(Array(1200L), clockSchema)),
      (1500L, new ExternalRow(Array(1500L), clockSchema)),
      (1600L, new ExternalRow(Array(1600L), clockSchema))
    )
    val clockTs = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(clockData, defaultNumPartitions), KeyPartitioningType.Sorted),
      clockSchema
    )
//    clockTs.cache()
//    clockTs.count()
//    clockTs.toDF.show()

    val clockTsExt = clockTs.addColumns(
      "snapshot" -> LongType -> {_ => null},
      "delta" -> LongType -> {_ => null},
      "generate" -> BooleanType -> {_ => true}
    )
    clockTsExt.cache()
    clockTsExt.count()
    clockTsExt.toDF.show()

    val mergedTs = ts.merge(clockTsExt)
    mergedTs.cache()
    mergedTs.count()
    mergedTs.toDF.show()

    val window = Windows.pastAbsoluteTime("500ns")
    val summarizedTs = mergedTs.summarizeWindows(window, SelectiveAccumulateSummarizerSpec.SummarizerFactory())
    summarizedTs.cache()
    summarizedTs.count()
    summarizedTs.toDF.show()
  }

}
