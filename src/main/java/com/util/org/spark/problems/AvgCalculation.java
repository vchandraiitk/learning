package com.util.org.spark.problems;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class AvgCalculation {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Dataset<Row> data = FileComparison.getDataSet("/Users/vikaschandra/IdeaProjects/data/salary").alias("salary");
        Dataset<Row> avg =  data.agg(avg("salary").alias("totalavg"));
        Dataset<Row> group =  data.groupBy("city").agg(avg("salary").alias("average"));
        group.crossJoin(avg).select(col("city"),col("average"), col("totalavg"), ((col("totalavg").minus(col("average"))).divide(col("totalavg")).multiply(100)).alias("calc")) .show();
    }
}
