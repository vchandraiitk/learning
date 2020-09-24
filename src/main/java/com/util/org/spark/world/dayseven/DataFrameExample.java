package com.util.org.spark.world.dayseven;

import com.util.org.spark.world.dayfive.JoinExample;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.soundex;

public class DataFrameExample {

    public static void main(String[] args) {
        //System.out.println(NAME);
        Logger.getLogger("org").setLevel(Level.ERROR);
        String s1 = "/Users/vikaschandra/IdeaProjects/gitlearning/dataset/orig";
        String s2 = "/Users/vikaschandra/IdeaProjects/gitlearning/dataset/dup";
        SparkSession sc = SparkSession.builder().appName("df").master("local").getOrCreate();
        DataFrameReader df = sc.read();
        Dataset<Row> dataset1 = df.option("header", true).csv(s1).alias("orig");
        Dataset<Row> dataset2 = df.option("header", true).csv(s2).alias("dup");
        dataset1.show();
        dataset2.show();
        //dataset1.join (dataset2, col("f._c0").equalTo(col("s._c0")), "fullouter").select("s._c0", "f._c1").show();
        //dataset.show(2);
        //dataset.filter(col("city").startsWith("b")).show();
        //dataset.select("id", "name").show();
        Dataset<Row> row= dataset1.join(dataset2, col("orig.id").equalTo( col("dup.id")), "fullouter")
                .select(col("orig.id").alias("origid"), col("dup.id").alias("dupid"), col("orig.name").alias("origname"), col("dup.amount").alias("amount"));
        //row.show();
        //row.groupBy("")
        long longds = row.filter(col("origid").isNull()).count();
        System.out.println(longds);
        System.out.println(row.filter(col("origid").isNotNull()).filter(col("dupid").isNotNull()).count() );
        String [] arr =  row.columns();
        for(String s : arr){
            System.out.println(s+":"+row.filter(col(s).isNull()).count());
        }
    }
}
