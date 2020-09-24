package com.util.org.spark.problems;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class FileComparison {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        String [] joinColumns = {"id"};
        Dataset<Row> left = getDataSet("/Users/vikaschandra/IdeaProjects/data/dataset/orig").alias("left");
        Dataset<Row> right = getDataSet("/Users/vikaschandra/IdeaProjects/data/dataset/dup").alias("right");

        Dataset<Row> innerJoin = getJoinResult(left, right, joinColumns, JoinType.FULL.getJoinVal());
        innerJoin.show();
        for(String c: left.columns()){
            innerJoin.filter(col("left_"+c).isNull().or(col("right_"+c).isNull()).or(col("left_"+c).notEqual (col("right_"+c)))).groupBy("left_"+c).count().show();
        }

        for(String c: left.columns()){
            innerJoin.filter(col("left_"+c).isNull().or(col("right_"+c).isNull()).or(col("left_"+c).notEqual (col("right_"+c)))).show();
        }

        //Dataset<Row> leftJoin = getJoinResult(left, right, joinColumns, JoinType.FULL.getJoinVal());
        //leftJoin.show();

    }

    public static Dataset<Row> getDataSet(String path){
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sc = SparkSession.builder().appName(System.currentTimeMillis()+"").master("local").getOrCreate();
        return sc.read().option("header", true).csv(path);
    }

    public static Dataset<Row> getJoinResult(Dataset<Row> left, Dataset<Row> right, String [] joinColumns,
                                             String joinType){
        return left.join(right, getKeyCols(joinColumns), joinType).select( getCols(left, right));

    }

    private static Column[] getCols(Dataset<Row> left, Dataset<Row> right){
        List<Column> list = new ArrayList<>();
        list.addAll(getCols(left, "left"));
        list.addAll(getCols(right, "right"));
        return list.toArray(new Column[list.size()]);
    }

    public static List<Column> getCols(Dataset<Row> dataset, String alias){
        String [] cols =  dataset.columns();

        List<Column> columnList = new ArrayList<>();
        for (String c : cols){
            columnList.add(col(alias+"."+c).alias(alias+"_"+c) );
        }
        return columnList;
    }

    public static Column getKeyCols(String [] cols){
        int counter = 0;
        Column col = null;
        for (String c : cols){
            if(counter == 0){
                col = col("left."+c).equalTo(col("right."+c));
            }else{
                col.and(col("left."+c).equalTo(col("right."+c)));
            }
            counter++;

        }
        return col;
    }
}
