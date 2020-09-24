package com.util.org.spark.world.dayfive;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SortingExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s = "/Users/vikaschandra/IdeaProjects/gitlearning/idsaldup";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.textFile(s);
        //rdd.collect().forEach(line -> System.out.println(line));
        //rdd.collect().forEach(System.);
        JavaPairRDD<String, String> pairRdd = rdd.mapToPair(p->new Tuple2<>(p.split(",")[1], p.split(",")[0]) );
        //pairRdd.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //JavaPairRDD<String, String> pairRdd1 = pairRdd.foreach(p-> new Tuple2<>(p._2(), p._1()));
        JavaPairRDD<String, String> pairRdd3 = pairRdd.sortByKey(true);
        JavaPairRDD<String, String> pairRdd4 = pairRdd3.mapToPair(p->new Tuple2<>(p._2(), p._1()));
        pairRdd4.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //pairRdd3.foreach(p-> System.out.println(p.swap()._1()));
        //pairRdd3.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));


    }
}
