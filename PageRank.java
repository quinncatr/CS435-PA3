// package ;

//common import statements amongst the stuff I saw 
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;



public class PageRank {
    // Calculation for PageRank algorithm, takes in int amount of iterations (25) and a double for the damping factor we wish to use (0.85)
    public static JavaPairRDD<Integer, Double> calculatePageRank(int iterations, double damping) {
    
        Doc doc = Book.mapPairs(); 
        JavaPairRDD<Integer, List<Integer>> links = doc.links;
        JavaPairRDD<Integer, String> titles = doc.title;
        long N = links._2().length();

        // Caches Links because they are reused for every iteration of the calculation
        links.cache();

        //initialize rank of each page to 1.0
        JavaPairRDD<Integer, Double> ranks = links.keys().mapToPair(page -> new Tuple2<>(page, 1.0));
        
        //Loop PageRank Iteratons
        for (int i = 0; i < iterations; i++ ) {
            // find contributions for a page/outgoing links

            // calculate PageRank for page and update corresponding ranks
        }
    }
}
