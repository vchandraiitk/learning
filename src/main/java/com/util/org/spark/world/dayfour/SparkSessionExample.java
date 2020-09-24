package com.util.org.spark.world.dayfour;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkSessionExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf(false);
        SparkContext sc1 = null;
        SparkContext sc2 = new SparkContext((conf));
        SparkSession s1 = new SparkSession(sc1);
        SparkSession s2 = new SparkSession(sc2);
        System.out.println(s1.hashCode() == s2.hashCode());
        System.out.println(s1.sparkContext().hashCode() == s2.sparkContext().hashCode());


    }
}
