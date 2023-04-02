/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cu.uci.e18a.spark;


import static org.apache.spark.sql.functions.col;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author desarrollo
 */
public class Cesarea {
    public static void main(String[] args) {
           //Crear contexto Spark con nombre de app y la url del master, local[*]
        SparkConf conf = new SparkConf().setAppName("Base de datos cesarea").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //Para trabajar con Dataframes o Dataset(bd distribuidas)
        SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();

        //crear dataset a partir de fichero csv
        Dataset<Row> df = spark.read().option("header", true).option("inferSchema", "true").csv("caesarian.csv");

        //para ver esquema y ver los primeros 10 registros
        df.printSchema();
        df.show(10);

        Dataset<Row> logregdataall = df.select(col("Age"),
                col("Delivery_number"), col("Delivery_time"), 
                col("Blood_Pressure"), col("Heart_Problem"),
                col("Cesarian").as("label"));
        
        logregdataall.show(10);
        
        //Contando por edad
        logregdataall.groupBy("Age").count().show();   
        
        //Eliminar valores ausentes
        Dataset<Row> logredata= logregdataall.na().drop();
        
        logredata.filter(col("Age").gt(18)).select(col("label"))
                .groupBy(col("label")).count().show();
        
        logredata.createOrReplaceTempView("cesarea");
        
        Dataset<Row> consuta = spark.sql("select label, count(*) as  cantidad from cesarea c where c.Age > 18 group by 1");
        consuta.show();
        
        logredata.show(10);
    }
    
}
