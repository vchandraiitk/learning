package com.util.org.spark.world.dayfive;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class JoinExample {
    public static final String NAME="VIKAS";
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s1 = "/Users/vikaschandra/IdeaProjects/gitlearning/idsalary";
        String s2 = "/Users/vikaschandra/IdeaProjects/gitlearning/idname";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd1 = sc.textFile(s1);
        JavaRDD<String> rdd2 = sc.textFile(s2);
        JavaPairRDD<String, String> idSalary = rdd1.mapToPair(p->new Tuple2<>(p.split(",")[0], p.split(",")[1]));
        JavaPairRDD<String, String> idname   = rdd2.mapToPair(p->new Tuple2<>(p.split(",")[0], p.split(",")[1]));


        //idSalary.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //idname.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //idSalary.join(idname).collect().forEach(p-> System.out.println(p._1+"-"+p._2()._1()+"#"+p._2()._2()));
        idSalary.fullOuterJoin(idname).collect().forEach(p-> System.out.println(p._1+"-"+p._2()._1()+"#"+p._2()._2()));
    }
}
