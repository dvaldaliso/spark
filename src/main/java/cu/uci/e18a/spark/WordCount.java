/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cu.uci.e18a.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *
 * @author desarrollo
 */
public class WordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("WordCount")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        
        JavaRDD<String> lines = spark.read().textFile("book.txt").javaRDD();
        JavaRDD<String> words = lines.flatMap(s->Arrays.asList(s.split(" ")).iterator());
        
        //Eliminando caracteres raros
        JavaRDD<String> preproc = words.map(s->{
            String t = s.replaceAll("[^a-zA-ZáéíóúñÁÉÍÓÚÑ]","");
            return t.toLowerCase();
        });
        
        JavaPairRDD<String, Integer> ones = words.mapToPair(s->new Tuple2<>(s,1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1,i2)->i1+i2);
        List<Tuple2<String, Integer>> tuples = new ArrayList<>(counts.collect());
        
         tuples.sort((t1,t2)->{
            Integer i1 = t1._2();
            Integer i2 = t2._2();
            return -1*i1.compareTo(i2);
        });
         
        for(Tuple2<?,?> tuple : tuples){
            System.out.println(tuple._1()+" "+tuple._2());
        }        
         spark.stop();
    }
    
}
