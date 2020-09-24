package com.util.org.spark.problems;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

abstract class MaxNumbers {
    abstract void doSomething();

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s1 = "/Users/vikaschandra/IdeaProjects/data/random";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        int counter = 0;
        JavaRDD<String> rdd1 = sc.textFile(s1);
        //rdd1.collect().forEach(System.out::println);
        JavaPairRDD<String, Integer> p1 = rdd1.mapToPair(p->new Tuple2<>(p, 1)).reduceByKey((x, y)->(x+y));
        JavaRDD<Row> row = p1.map (p->RowFactory.create(p._1(),p._2()));
        SparkSession ss = SparkSession.builder().master("local").appName("ds"). getOrCreate();
        StructType structType = DataTypes
                .createStructType(
                        new StructField[]{
                                DataTypes.createStructField("num", DataTypes.StringType, true)
                                , DataTypes.createStructField("counter", DataTypes.IntegerType, true)});

        Dataset<Row> ds= ss.sqlContext().createDataFrame(row, structType).toDF();
        //System.out.println();

        ds.filter(col("counter").geq( ds.agg(max(col("counter"))).head().getInt(0))).show();
    }
}
