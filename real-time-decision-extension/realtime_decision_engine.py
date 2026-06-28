"""Real-time business decision layer for telematics events.

This script extends the base project with a real-time intervention engine.
It reads raw telematics events from Kafka, detects immediate and short-window
risk patterns, and publishes business decisions to the driver_interventions
topic.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 real_time_decision_extension/realtime_decision_engine.py
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    avg,
    col,
    concat,
    count,
    current_timestamp,
    expr,
    from_json,
    greatest,
    lit,
    round as spark_round,
    struct,
    sum as spark_sum,
    to_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType


RAW_TOPIC = "telematics_raw"
DECISION_TOPIC = "driver_interventions"
BOOTSTRAP = "broker:9092"


def event_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("driver_profile", StringType(), True),
            StructField("event_time", StringType(), False),
            StructField("speed_kmh", DoubleType(), False),
            StructField("speed_limit_kmh", DoubleType(), False),
            StructField("acceleration_ms2", DoubleType(), False),
            StructField("braking_ms2", DoubleType(), False),
            StructField("cornering_g", DoubleType(), False),
            StructField("road_type", StringType(), False),
            StructField("weather", StringType(), False),
            StructField("is_night", BooleanType(), False),
            StructField("distance_delta_km", DoubleType(), False),
            StructField("phone_usage", BooleanType(), False),
        ]
    )


def add_risk_flags(df):
    return (
        df.withColumn("is_speeding", col("speed_kmh") > col("speed_limit_kmh") + lit(10))
        .withColumn("speeding_margin", greatest(col("speed_kmh") - col("speed_limit_kmh"), lit(0.0)))
        .withColumn("is_hard_braking", col("braking_ms2") < lit(-3.0))
        .withColumn("is_extreme_braking", col("braking_ms2") < lit(-5.0))
        .withColumn("is_harsh_acceleration", col("acceleration_ms2") > lit(2.5))
        .withColumn("is_sharp_cornering", col("cornering_g") > lit(0.35))
        .withColumn("is_night_risk", col("is_night") & (col("speed_kmh") > col("speed_limit_kmh")))
        .withColumn("is_bad_weather", col("weather").isin("rain", "fog", "snow"))
        .withColumn("is_phone_usage", col("phone_usage"))
        .withColumn(
            "risk_score_event",
            col("is_speeding").cast("int")
            + col("is_hard_braking").cast("int")
            + col("is_harsh_acceleration").cast("int")
            + col("is_sharp_cornering").cast("int")
            + col("is_night_risk").cast("int")
            + col("is_bad_weather").cast("int")
            + col("is_phone_usage").cast("int"),
        )
        .withColumn("risk_event", col("risk_score_event") > 0)
        .withColumn(
            "possible_crash_or_incident",
            col("is_extreme_braking")
            | ((col("braking_ms2") < lit(-4.2)) & (col("speed_kmh") > lit(60)) & col("is_sharp_cornering")),
        )
    )


def active_rule_array():
    return array(
        when(col("is_speeding"), lit("speeding")),
        when(col("is_hard_braking"), lit("hard_braking")),
        when(col("is_extreme_braking"), lit("extreme_braking")),
        when(col("is_harsh_acceleration"), lit("harsh_acceleration")),
        when(col("is_sharp_cornering"), lit("sharp_cornering")),
        when(col("is_night_risk"), lit("night_speed_risk")),
        when(col("is_bad_weather"), lit("bad_weather")),
        when(col("is_phone_usage"), lit("phone_usage")),
    )


def build_event_interventions(flagged):
    decision_type = (
        when(col("possible_crash_or_incident"), lit("CHECK_DRIVER_STATUS"))
        .when(col("risk_score_event") >= lit(4), lit("SEND_SAFETY_NUDGE"))
        .otherwise(lit(None))
    )
    priority = (
        when(col("possible_crash_or_incident"), lit("critical"))
        .when(col("risk_score_event") >= lit(5), lit("high"))
        .otherwise(lit("medium"))
    )
    recommended_action = (
        when(
            col("possible_crash_or_incident"),
            lit("Send push/SMS asking if the driver is safe and prepare assistance triage."),
        )
        .otherwise(lit("Send an in-app safety nudge during the trip."))
    )
    customer_message = (
        when(
            col("possible_crash_or_incident"),
            lit("Wykryto gwaltowne zdarzenie. Czy wszystko w porzadku?"),
        )
        .otherwise(lit("W ostatnich minutach jazda wyglada ryzykownie. Zwolnij i zachowaj wiekszy odstep."))
    )

    enriched = (
        flagged.withColumn("decision_type", decision_type)
        .withColumn("priority", priority)
        .withColumn("recommended_action", recommended_action)
        .withColumn("customer_message", customer_message)
        .withColumn("active_rules_raw", active_rule_array())
        .withColumn("active_rules", expr("filter(active_rules_raw, x -> x is not null)"))
        .filter(col("decision_type").isNotNull())
    )

    payload = to_json(
        struct(
            lit("event").alias("decision_source"),
            current_timestamp().alias("decision_ts"),
            "event_id",
            "driver_id",
            "vehicle_id",
            "driver_profile",
            "event_time_ts",
            "decision_type",
            "priority",
            "recommended_action",
            "customer_message",
            "active_rules",
            "risk_score_event",
            "speed_kmh",
            "speed_limit_kmh",
            "braking_ms2",
            "cornering_g",
            "road_type",
            "weather",
            "is_night",
            "phone_usage",
        )
    )
    return enriched.select(col("driver_id").alias("key"), payload.alias("value"))


def build_window_features(flagged, duration: str, slide: str):
    grouped = flagged.withWatermark("event_time_ts", "30 seconds").groupBy(
        col("driver_id"), window(col("event_time_ts"), duration, slide)
    )
    return (
        grouped.agg(
            count("*").alias("event_count"),
            spark_round(spark_sum("distance_delta_km"), 4).alias("km_driven"),
            spark_round(avg("speed_kmh"), 2).alias("avg_speed"),
            spark_sum(col("risk_event").cast("int")).alias("risk_event_count"),
            spark_sum("risk_score_event").alias("risk_score_5m"),
            spark_sum(col("is_speeding").cast("int")).alias("speeding_count"),
            spark_sum(col("is_hard_braking").cast("int")).alias("hard_braking_count"),
            spark_sum(col("is_harsh_acceleration").cast("int")).alias("harsh_acceleration_count"),
            spark_sum(col("is_sharp_cornering").cast("int")).alias("sharp_cornering_count"),
            spark_sum(col("is_bad_weather").cast("int")).alias("bad_weather_event_count"),
            spark_sum(col("is_night").cast("int")).alias("night_event_count"),
            spark_sum(col("phone_usage").cast("int")).alias("phone_usage_count"),
        )
        .withColumn("risk_intensity", col("risk_event_count") / col("event_count"))
        .withColumn("risk_events_per_100km", col("risk_event_count") / greatest(col("km_driven"), lit(0.001)) * lit(100.0))
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )


def build_window_interventions(windowed):
    decision_type = (
        when((col("risk_score_5m") >= lit(12)) | (col("phone_usage_count") >= lit(3)), lit("ESCALATE_HIGH_RISK_REVIEW"))
        .when((col("event_count") >= lit(5)) & (col("risk_intensity") >= lit(0.45)), lit("SEND_SAFETY_NUDGE"))
        .when((col("event_count") >= lit(10)) & (col("risk_event_count") == lit(0)) & (col("km_driven") >= lit(0.25)), lit("GRANT_SAFE_DRIVING_POINTS"))
        .otherwise(lit(None))
    )
    priority = (
        when(col("decision_type") == lit("ESCALATE_HIGH_RISK_REVIEW"), lit("high"))
        .when(col("decision_type") == lit("SEND_SAFETY_NUDGE"), lit("medium"))
        .otherwise(lit("low"))
    )
    recommended_action = (
        when(
            col("decision_type") == lit("ESCALATE_HIGH_RISK_REVIEW"),
            lit("Flag this active trip for operational review and possible outbound contact."),
        )
        .when(
            col("decision_type") == lit("SEND_SAFETY_NUDGE"),
            lit("Send a real-time safety nudge before the risky pattern becomes a claim."),
        )
        .otherwise(lit("Add safe-driving points to the customer's reward wallet."))
    )
    customer_message = (
        when(
            col("decision_type") == lit("GRANT_SAFE_DRIVING_POINTS"),
            lit("Dobra, spokojna jazda. Dodajemy punkty safe driving do Twojego konta."),
        )
        .otherwise(lit("Widzimy podwyzszone ryzyko tej podrozy. Jedz ostrozniej, zeby utrzymac bonus za bezpieczna jazde."))
    )

    enriched = (
        windowed.withColumn("decision_type", decision_type)
        .filter(col("decision_type").isNotNull())
        .withColumn("priority", priority)
        .withColumn("recommended_action", recommended_action)
        .withColumn("customer_message", customer_message)
        .withColumn(
            "decision_reason",
            concat(
                lit("5-minute window: events="),
                col("event_count").cast("string"),
                lit(", risk_events="),
                col("risk_event_count").cast("string"),
                lit(", risk_score="),
                col("risk_score_5m").cast("string"),
                lit(", phone_usage="),
                col("phone_usage_count").cast("string"),
            ),
        )
    )

    payload = to_json(
        struct(
            lit("sliding_window_5m").alias("decision_source"),
            current_timestamp().alias("decision_ts"),
            "driver_id",
            "window_start",
            "window_end",
            "decision_type",
            "priority",
            "recommended_action",
            "customer_message",
            "decision_reason",
            "event_count",
            "km_driven",
            "avg_speed",
            spark_round(col("risk_intensity"), 3).alias("risk_intensity"),
            spark_round(col("risk_events_per_100km"), 2).alias("risk_events_per_100km"),
            "risk_event_count",
            "risk_score_5m",
            "speeding_count",
            "hard_braking_count",
            "harsh_acceleration_count",
            "sharp_cornering_count",
            "bad_weather_event_count",
            "night_event_count",
            "phone_usage_count",
        )
    )
    return enriched.select(col("driver_id").alias("key"), payload.alias("value"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default=BOOTSTRAP)
    parser.add_argument("--raw-topic", default=RAW_TOPIC)
    parser.add_argument("--decision-topic", default=DECISION_TOPIC)
    parser.add_argument("--checkpoint-dir", default="data/checkpoints/realtime_decisions")
    parser.add_argument("--show-console", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("TelematicsRealTimeDecisionEngine").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.raw_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), event_schema()).alias("event"))
        .select("event.*")
        .withColumn("event_time_clean", expr("regexp_replace(event_time, 'Z$', '+00:00')"))
        .withColumn("event_time_ts", to_timestamp(col("event_time_clean"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        .drop("event_time_clean")
    )
    flagged = add_risk_flags(parsed)

    event_interventions = build_event_interventions(flagged)
    event_query = (
        event_interventions.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("topic", args.decision_topic)
        .option("checkpointLocation", f"{args.checkpoint_dir}/event_interventions")
        .outputMode("append")
        .start()
    )

    windowed = build_window_features(flagged, "5 minutes", "1 minute")
    window_interventions = build_window_interventions(windowed)
    window_query = (
        window_interventions.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("topic", args.decision_topic)
        .option("checkpointLocation", f"{args.checkpoint_dir}/window_interventions")
        .outputMode("update")
        .start()
    )

    queries = [event_query, window_query]
    if args.show_console:
        console_query = (
            window_interventions.selectExpr("CAST(value AS STRING)")
            .writeStream.format("console")
            .option("truncate", False)
            .outputMode("update")
            .start()
        )
        queries.append(console_query)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
