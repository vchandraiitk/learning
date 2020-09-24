package com.util.org.spark.problems;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

abstract class DistinctWithRank {
    abstract void doSomething();

    public static void main(String[] args) {
        Dataset<Row> row = FileComparison.getDataSet("/Users/vikaschandra/IdeaProjects/data/rank").alias("t");
        //row.show();
        row.groupBy(col("s"), col("b")).agg(min(col("rank")).alias("rank"))
                .select(col("rank"), col("s"),col("b"))
                .sort(col("rank"), col("s"),col("b")).show();
    }
}
