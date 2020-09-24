package com.util.org.spark.problems;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Movies {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String path = "/Users/vikaschandra/IdeaProjects/data/movies";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
         JavaRDD<String> rdd1 = sc.textFile(path);
         rdd1.filter(p->p.substring(p.length()-4).equals("1973")).collect().forEach(System.out::println);
    }
}
