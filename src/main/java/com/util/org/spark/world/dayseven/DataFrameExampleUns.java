package com.util.org.spark.world.dayseven;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class DataFrameExampleUns {

    public static void main(String[] args) {
        //System.out.println(NAME);
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s = "/Users/vikaschandra/sparktest/unst";
        SparkSession sc = SparkSession.builder().appName("df").master("local").getOrCreate();
        DataFrameReader df = sc.read();
        Dataset<Row> dataset = df.option("header", true).csv(s);
        dataset.show();

    }
}
