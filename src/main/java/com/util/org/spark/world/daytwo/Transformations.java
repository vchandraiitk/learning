package com.util.org.spark.world.daytwo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Transformations {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s = "/Users/vikaschandra/sparktest/employee1";
        String s1 = "/Users/vikaschandra/sparktest/employee2";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[3]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd1 = sc.textFile(s);
        rdd1.collect().forEach(System.out::println);
        System.out.println("--------");

        JavaRDD<String> rdd2 = sc.textFile(s1);
        rdd2.collect().forEach(System.out::println);
        System.out.println("--------");
        sample(rdd1, rdd2);
    }

   public static void union(JavaRDD<String> rdd1, JavaRDD<String> rdd2){
        rdd1.union(rdd2).collect().forEach(System.out::println);
   }

    public static void inter(JavaRDD<String> rdd1, JavaRDD<String> rdd2){
        rdd1.intersection(rdd2).collect().forEach(System.out::println);
    }
    public static void distinct(JavaRDD<String> rdd1, JavaRDD<String> rdd2){
        rdd1.union(rdd2).distinct().collect().forEach(System.out::println);
        System.out.println("--------");
        rdd1.union(rdd2).distinct().collect().forEach(System.out::println);
        System.out.println("--------");
        rdd1.union(rdd2).distinct().collect().forEach(System.out::println);
        System.out.println("--------");
        rdd1.union(rdd2).distinct().collect().forEach(System.out::println);
        System.out.println("--------");
    }

    public static void take(JavaRDD<String> rdd1, JavaRDD<String> rdd2){
        rdd1.union(rdd2).take(2).forEach(System.out::println);
    }

    public static void sample(JavaRDD<String> rdd1, JavaRDD<String> rdd2){
        rdd1.union(rdd2).sample(true, 1).collect().forEach(System.out::println);
    }


}
