package com.util.org.spark.world.dayfour;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

public class PaairRddExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s = "/Users/vikaschandra/IdeaProjects/gitlearning/names";
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.textFile(s);
        //rdd.collect().forEach(System.out::println);
        //JavaPairRDD<Integer, String> pairRDD = rdd.mapToPair(p->new Tuple2<>(Integer.parseInt(p.split(",")[0]), p.split(",")[1]));
        //pairRDD.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //JavaPairRDD<Integer, String> pairRDD1 =  pairRDD.filter(p->p._2().startsWith("v"));
        //pairRDD1.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //pairRDD.collectAsMap().entrySet().forEach(p-> System.out.println(p.getKey()+":"+p.getValue()));
        //pairRDD.countByValue().entrySet().forEach(p-> System.out.println(p.getKey()+":"+p.getValue()));

        //JavaPairRDD<Integer, String> pairRDDCounter = rdd.mapToPair(p->new Tuple2<>(Integer.parseInt(p.split(",")[0]), p.split(",")[1]));
        JavaPairRDD<String, Integer> pairRDDCounter = rdd.mapToPair(p->new Tuple2<>(p.split(",")[1], Integer.parseInt(p.split(",")[0])));

        pairRDDCounter.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        System.out.println("-----");
        pairRDDCounter.sortByKey(true).foreach(p-> System.out.println(p._1()+":"+p._2()));

       // pairRDDCounter.groupByKey().collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        System.out.println("------");

       // revisit
      //  pairRDDCounter.groupBy(p->new Tuple2(p._1(),p._2())).collect().forEach(p-> System.out.println(p._1()+"#"+p._2()));


      //  System.out.println("------");
      //  pairRDDCounter.reduceByKey((x,y)->(x+":"+y)).collect().forEach(p-> System.out.println(p._1()+"#"+p._2()));
      //  System.out.println("------");
       // pairRDDCounter.reduceByKey((x,y)->(x+":"+y)).collect().forEach(p-> System.out.println(p._1()+":"+p._2()));

        //pairRDDCounter.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //JavaPairRDD<Integer, String> reduceByKeyPairRdd = pairRDDCounter.reduceByKey((x,y)->(x+":"+y));
        //reduceByKeyPairRdd.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //reduceByKeyPairRdd


       // JavaPairRDD<String, String> mapToPairRdd = rdd.mapToPair(p -> new Tuple2<>(p.split(",")[0], p.split(",")[1]));
       // JavaPairRDD<String, Integer> mapToPairRdd1 = rdd.mapToPair(p -> new Tuple2<>(p.split(",")[0], 1));
        //mapToPairRdd.collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
        //JavaPairRDD<String, String> filterPair = mapToPairRdd.filter(p->p._2().startsWith("v"));
        //filterPair.collect().forEach(p-> System.out.println(p._1()));
        //filterPair.reduceByKey((x,y)->(x+y)).collect().forEach(p-> System.out.println(p._1+":"+p._2));
       // mapToPairRdd.countByValue().entrySet().forEach(p-> System.out.println(p.getKey()+":"+p.getValue()));
       // mapToPairRdd.collectAsMap().entrySet().forEach(p-> System.out.println(p.getKey()+":"+p.getValue()));
       // System.out.println("------------");
       // mapToPairRdd.reduceByKey((x,y)->(1+"")).collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
       // mapToPairRdd1.reduceByKey((x,y)->(x+y)).collect().forEach(p-> System.out.println(p._1()+":"+p._2()));
    }
}
