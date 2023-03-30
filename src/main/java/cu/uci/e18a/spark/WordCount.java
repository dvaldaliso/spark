
package cu.uci.e18a.spark;

import org.apache.spark.sql.SparkSession;

/**
 *
 * @author desarrollo
 */
public class WordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder()
                .appName("WordCount")
                .master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        
    }
}
