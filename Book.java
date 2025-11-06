//package ;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.io.BytesWritable;

public class Book 
{
    public static class Doc 
    {
        public final JavaPairRDD<Integer, String> links;
        public final JavaPairRDD<Integer, String> title;

        public Doc(JavaPairRDD<Integer, String> links, JavaPairRDD<Integer, String> title) 
        {
            this.links = links;
            this.title = title;
        }
    }

    public static Doc mapPairs(BytesWritable bytes) 
    {
        JavaRDD<String> links = sc.textFile("hdfs://PA3/Demo-Dataset/links-simple-sorted-sample.txt");
        JavaRDD<String> title_txt = sc.textFile("hdfs://PA3/Demo-Dataset/titles-sorted-sample.txt");
        JavaPairRDD<Integer, List<Integer>> pairs = links.mapToPair(s ->
        {
            String[] line = s.split(":");
            int page = Integer.parseInt(line[0].trim());
            List<Integer> ranks = new ArrayList<>();

            if (line.length > 1 && !line[1].trim().isEmpty())
                for (String l : line[1].trim().split("\\s+"))
                    ranks.add(Integer.parseInt(l));

            return new Tuple2<>(page, ranks); // takes the starting pg number as the key and the page ranks as the values
        });

        JavaPairRDD<Integer, String> title = title_txt.zipWithIndex().mapToPair(s -> new Tuple2<>(s._2.intValue() + 1, t._1)); // ._1() getter method

        return new Doc(pairs, title);
    }

    public static JavaPairRDD<Integer, String> getTitle(){
        return title;
    }

    public static JavaPairRDD<Integer, String> getLinks(){
        return links;
    }

}
