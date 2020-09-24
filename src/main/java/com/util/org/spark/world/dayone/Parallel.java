package com.util.org.spark.world.dayone;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Parallel {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[6]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,89,3,4,4,7,8,89,3,4,4,35,646,4,6,64,64), 9);
        //rdd.collect().forEach(System.out::println);

        System.out.println(rdd.getNumPartitions());

    }
}
