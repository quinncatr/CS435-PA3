//package ;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.io.BytesWritable;

public class Book 
{
    public static class Doc 
    {
        public final String id;
        public final String title;
        public final String body;

        public Doc(String id, String title, String body) 
        {
            this.id = id;
            this.title = title;
            this.body = body;
        }
    }

    public static List<Doc> parseAll(BytesWritable bytes) 
    {
        JavaRDD<String> links = sc.textFile("hdfs://PA3/Demo-Dataset/links-simple-sorted-sample.txt");
        JavaRDD<String> titles = sc.textFile("hdfs://PA3/Demo-Dataset/titles-sorted-sample.txt");
        JavaPairRDD<Integer, List<Integer>> pairs = links.mapToPair(s ->
        {
            String[] line = s.split(":");
            int page = Integer.parseInt(line[0].trim());
            List<Integer> ranks = new ArrayList<>();

            if (line.length > 1 && !line[1].trim().isEmpty())
                for (String l : line[1].trim().split("\\s+"))
                    ranks.add(Integer.parseInt(l));

            return new Tuple2<>(page, ranks);
        });
        // what do we reduce by?
        
        return out;
    }

    public static String getTitle(){
        /* Get the title of the wiki article:
        To find the page title that corresponds to
        integer n, just look up the n-th line in the file
        titles-sorted.txt */

        return title;
    }

}
