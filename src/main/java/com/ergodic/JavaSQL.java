package com.ergodic;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaSQL {

    public static void main(String[] args) {
        // $example on:init_session$
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
//                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        // $example off:init_session$
        Dataset<Row> df = spark.read().json("src/main/resources/people.json");

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();

        spark.stop();
    }
}
