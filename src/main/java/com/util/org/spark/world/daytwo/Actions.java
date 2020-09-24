package com.util.org.spark.world.daytwo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.Map;

public class Actions {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s = "/Users/vikaschandra/IdeaProjects/gitlearning/names";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.textFile(s);
        wordCount(rdd);
        //mapExample2(rdd);
    }

    public static void collect(JavaRDD<String> rdd){
        rdd.collect().forEach(System.out::println);

    }

    public static void count(JavaRDD<String> rdd){
        System.out.println(rdd.count());
    }

    public static void countByValue(JavaRDD<String> rdd){
        System.out.println(rdd.countByValue());
    }

    public static void map(JavaRDD<String> rdd){
        rdd.map( p -> p.length()).collect().forEach(System.out::println);
    }

    public static void mapExample2(JavaRDD<String> rdd){
        rdd.map( p -> p.split(",").length).collect().forEach(System.out::println);
    }

    public static void flatMap(JavaRDD<String> rdd){
        rdd.flatMap(p -> Arrays.asList(p.split(",")).iterator()).collect().forEach(System.out::println);
    }

    public static void wordCount(JavaRDD<String> rdd){
        rdd.flatMap(p -> Arrays.asList(p.split(",")).iterator()).countByValue().entrySet().forEach(p-> System.out.println(p.getKey() + " : "+ p.getValue()));
    }



}
