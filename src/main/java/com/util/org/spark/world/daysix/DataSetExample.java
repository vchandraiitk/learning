package com.util.org.spark.world.daysix;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class DataSetExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s = "/Users/vikaschandra/sparktest/employee1";
        SparkSession sc = SparkSession.builder().master("local").appName("vikas").getOrCreate();
        DataFrameReader df = sc.read();
        Dataset<Row> dr = df.option("header", true).csv(s);
        dr.printSchema();
        dr.show();
        dr.select("id","name").show();
        dr.filter(col("id").equalTo("1")).show();

        dr.groupBy(col("id")).count().show();


    }
}
