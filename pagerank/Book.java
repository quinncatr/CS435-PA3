package pagerank;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.io.BytesWritable;

public class Book 
{

    public static class Doc 
    {
        public final JavaPairRDD<Integer, List<Integer>> links;
        public final JavaPairRDD<Integer, String> titles;

        public Doc(JavaPairRDD<Integer, List<Integer>> links, JavaPairRDD<Integer, String> titles) 
        {
            this.links = links;
            this.titles = titles;
        }
    }

    public static Doc mapPairs(JavaSparkContext sc, String linksPath, String titlesPath) 
    {
        JavaRDD<String> links = sc.textFile(linksPath);
        JavaRDD<String> title_txt = sc.textFile(titlesPath);
        JavaPairRDD<Integer, List<Integer>> pairs = links.mapToPair(s -> 
        {
            String[] line = s.split(":");
            int page = Integer.parseInt(line[0].trim());
            List<Integer> ranks = new ArrayList<>();

            if(line.length > 1 && !line[1].trim().isEmpty()) 
            {
                for(String l : line[1].trim().split("\\s+")) 
                {
                    ranks.add(Integer.parseInt(l));
                }
            }
            return new Tuple2<>(page, ranks); // takes the starting pg number as the key and the page ranks as the values
        });

        // The value pair is the line number as the kay and the title as the value, this means the keys match for pairs (above) and title (below)
        JavaPairRDD<Integer, String> titles = title_txt.zipWithIndex()
                .mapToPair(s -> new Tuple2<>((int) (s._2 + 1), s._1)); // ._1() getter method

        return new Doc(pairs, titles);
    }
}
