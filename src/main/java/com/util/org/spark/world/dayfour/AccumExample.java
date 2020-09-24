package com.util.org.spark.world.dayfour;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AccumExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s = "/Users/vikaschandra/IdeaProjects/gitlearning/idname";
        String s1 = "/Users/vikaschandra/IdeaProjects/gitlearning/idsalary";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        int counter = 0;
        JavaRDD<String> rdd1 = sc.textFile(s);
        //JavaRDD<String> rdd2 = sc.textFile(s1);
        rdd1.filter( p->{
            String [] arr = p.split(",");
            //counter = counter+1;
            return false;
        });

    }
}
