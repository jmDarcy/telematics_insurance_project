"""Spark Structured Streaming pipeline for telematics events.

Submit examples:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming_features.py
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2 spark_streaming_features.py
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    avg,
    col,
    concat_ws,
    count,
    expr,
    from_json,
    greatest,
    lit,
    max as spark_max,
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
ALERT_TOPIC = "telematics_alerts"
FEATURE_TOPIC = "driver_features"
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
    )


def build_alerts(flagged):
    rules = array(
        when(col("is_speeding"), lit("speeding")),
        when(col("is_hard_braking"), lit("hard_braking")),
        when(col("is_harsh_acceleration"), lit("harsh_acceleration")),
        when(col("is_sharp_cornering"), lit("sharp_cornering")),
        when(col("is_night_risk"), lit("night_speed_risk")),
        when(col("is_bad_weather"), lit("bad_weather")),
        when(col("is_phone_usage"), lit("phone_usage")),
    )
    return (
        flagged.filter(col("risk_event"))
        .withColumn("active_rules_raw", rules)
        .withColumn("active_rules", expr("filter(active_rules_raw, x -> x is not null)"))
        .select(
            col("driver_id").alias("key"),
            to_json(
                struct(
                    "event_id",
                    "driver_id",
                    "vehicle_id",
                    "event_time_ts",
                    "active_rules",
                    "risk_score_event",
                    "speed_kmh",
                    "speed_limit_kmh",
                    "acceleration_ms2",
                    "braking_ms2",
                    "cornering_g",
                    "road_type",
                    "weather",
                    "is_night",
                    "distance_delta_km",
                    "phone_usage",
                )
            ).alias("value"),
        )
    )


def aggregate_window(flagged, duration: str, slide: str | None):
    window_col = window(col("event_time_ts"), duration, slide) if slide else window(col("event_time_ts"), duration)
    grouped = flagged.withWatermark("event_time_ts", "30 seconds").groupBy(col("driver_id"), window_col)
    return (
        grouped.agg(
            count("*").alias("event_count"),
            spark_round(spark_sum("distance_delta_km"), 4).alias("km_driven"),
            spark_round(avg("speed_kmh"), 2).alias("avg_speed"),
            spark_max("speed_kmh").alias("max_speed"),
            spark_sum(col("is_speeding").cast("int")).alias("speeding_count"),
            spark_sum(col("is_hard_braking").cast("int")).alias("hard_braking_count"),
            spark_sum(col("is_harsh_acceleration").cast("int")).alias("harsh_acceleration_count"),
            spark_sum(col("is_sharp_cornering").cast("int")).alias("sharp_cornering_count"),
            spark_sum(col("is_night").cast("int")).alias("night_event_count"),
            spark_sum(col("is_bad_weather").cast("int")).alias("bad_weather_event_count"),
            spark_sum(col("phone_usage").cast("int")).alias("phone_usage_count"),
            spark_sum(col("risk_event").cast("int")).alias("risk_event_count"),
        )
        .withColumn("speeding_ratio", col("speeding_count") / col("event_count"))
        .withColumn("night_event_ratio", col("night_event_count") / col("event_count"))
        .withColumn("bad_weather_event_ratio", col("bad_weather_event_count") / col("event_count"))
        .withColumn("exposure_km", greatest(col("km_driven"), lit(0.001)))
        .withColumn("risk_events_per_100km", col("risk_event_count") / col("exposure_km") * lit(100.0))
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default=BOOTSTRAP)
    parser.add_argument("--raw-topic", default=RAW_TOPIC)
    parser.add_argument("--alert-topic", default=ALERT_TOPIC)
    parser.add_argument("--feature-topic", default=FEATURE_TOPIC)
    parser.add_argument("--output-dir", default="data/historical_features")
    parser.add_argument("--checkpoint-dir", default="data/checkpoints")
    parser.add_argument("--write-feature-topic", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("TelematicsInsuranceStreaming").getOrCreate()
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
    flagged.printSchema()

    alert_query = (
        build_alerts(flagged)
        .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("topic", args.alert_topic)
        .option("checkpointLocation", f"{args.checkpoint_dir}/alerts")
        .outputMode("append")
        .start()
    )

    console_query = (
        flagged.select("event_id", "driver_id", "event_time_ts", "speed_kmh", "speed_limit_kmh", "risk_score_event")
        .writeStream.format("console")
        .option("truncate", False)
        .outputMode("append")
        .start()
    )

    tumbling = aggregate_window(flagged, "1 minute", None)
    parquet_query = (
        tumbling.writeStream.format("parquet")
        .option("path", f"{args.output_dir}/tumbling_1m")
        .option("checkpointLocation", f"{args.checkpoint_dir}/features_tumbling_1m")
        .outputMode("append")
        .start()
    )

    sliding = aggregate_window(flagged, "5 minutes", "1 minute")
    sliding_console_query = (
        sliding.writeStream.format("console")
        .option("truncate", False)
        .outputMode("update")
        .start()
    )

    queries = [alert_query, console_query, parquet_query, sliding_console_query]
    if args.write_feature_topic:
        feature_query = (
            tumbling.select(col("driver_id").alias("key"), to_json(struct("*")).alias("value"))
            .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_server)
            .option("topic", args.feature_topic)
            .option("checkpointLocation", f"{args.checkpoint_dir}/features_kafka")
            .outputMode("append")
            .start()
        )
        queries.append(feature_query)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
