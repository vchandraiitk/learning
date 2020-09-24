package com.util.org.spark.world.dayone;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class ReadFileExample {

    public static void main(String[] args) {
        System.out.println(String.format("%1$s%2$s", "/mapr/", "EDLDEV2"));
        String s = "/Users/vikaschandra/Downloads/1.csv";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SparkSession ss = null;

        JavaRDD<String> rdd = sc.textFile(s);

        List<String> list =  rdd.collect();

        System.out.println(list.size());
        list.forEach(System.out::println);


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("com.databricks.spark.csv").option("header", true).load(s);
        df.show();



        Dataset<Row> df1 = df.filter(df.col("id").notEqual("1"));
        df1.show();



        Dataset<Row> df2 = df1.drop("marks");
        df2.show();



    }
}
