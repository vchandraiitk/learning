package com.util.org.spark.world.daythree;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PairRdd {
    public static void main(String[] args) {
        Tuple2<Integer, String> t = new Tuple2(1, "vikas");
        System.out.println(t._1());
        System.out.println(t._2);

        Tuple3<Integer, String, String> t3 = new Tuple3<>(1,"vikas", "chandra");
        System.out.println(t3._1());
        System.out.println(t3._2());
        System.out.println(t3._3());

        List<String> list = Arrays.asList(new String[]{"1, Vikas", "2,John", "3,Peter", "4,John"});
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, String> pairRdd =  sc.parallelize(list).mapToPair(p-> new Tuple2<>(p.split(",")[0], p.split(",")[1]));
        Map<String, String> map =  pairRdd.collectAsMap();
        map.entrySet().forEach(p-> System.out.println(p.getKey() +" : "+ p.getValue()));
    }
}
