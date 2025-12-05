package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {
        SparkSession ss=SparkSession.builder()
                .appName("Structured streaming App")
                .getOrCreate();
        StructType schema = new StructType(new StructField[]{
                new StructField("order_id",DataTypes.LongType,false, Metadata.empty()),
                new StructField("client_id",DataTypes.LongType,false, Metadata.empty()),
                new StructField("client_name",DataTypes.StringType,false, Metadata.empty()),
                new StructField("product",DataTypes.StringType,false, Metadata.empty()),
                new StructField("quantity",DataTypes.IntegerType,false, Metadata.empty()),
                new StructField("price",DataTypes.DoubleType,false, Metadata.empty()),
                new StructField("order_date",DataTypes.StringType,false, Metadata.empty()),
                new StructField("status",DataTypes.StringType,false, Metadata.empty()),
                new StructField("total",DataTypes.DoubleType,false, Metadata.empty())
        });

        Dataset<Row> inputDF = ss.readStream().schema(schema).option("header",true).csv("hdfs://namenode:8020/data");

        StreamingQuery query= inputDF.writeStream().format("console")
                .outputMode(OutputMode.Append())
                .start();
        query.awaitTermination();

    }
}