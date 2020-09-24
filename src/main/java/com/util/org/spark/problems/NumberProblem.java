package com.util.org.spark.problems;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class NumberProblem {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s = "/Users/vikaschandra/IdeaProjects/data/dataset/num";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.textFile(s);
        JavaRDD<String> rdd1 = rdd.flatMap(p-> Arrays.asList(p.split(",")).iterator());
        JavaRDD<Integer> rddmap = rdd1.map(p->Integer.parseInt(p));
        System.out.println(rddmap.reduce((x,y)->x+y));
       // JavaRDD<Integer> rdd3 =  rdd1.reduce((x,y)-> Integer.parseInt(x)+Integer.parseInt(y));
    }
}
