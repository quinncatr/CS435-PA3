package pagerank;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import pagerank.Book;
import scala.Tuple2;
import java.io.IOException;
import java.util.*;

public class PageRank 
{
    // Calculation for PageRank algorithm, takes in int amount of iterations (25) and a double for the damping factor we wish to use (0.85)
    public static JavaPairRDD<Integer, Double> calculatePageRank(JavaPairRDD<Integer, List<Integer>> links, int iterations, double damping, boolean taxation, long N) 
    {
        // Caches Links because they are reused for every iteration of the calculation
        links.cache();

        //initialize rank of each page to 1/N
        JavaPairRDD<Integer, Double> ranks = links.keys().mapToPair(page -> new Tuple2<>(page, 1.0 / N));

        for(int i = 0; i < iterations; i++) 
        {
            JavaPairRDD<Integer, Double> value = links.join(ranks).flatMapToPair(t -> {
                List<Integer> outlinks = t._2._1; //pages this page links to
                double rank = t._2._2; //current rank of page
                List<Tuple2<Integer, Double>> out = new ArrayList<>();

                //if page has no outgoing links i.e. deadend
                if(outlinks.isEmpty()) 
                {
                    out.add(new Tuple2<>(-1, rank));
                    return out.iterator();
                }

                //divide the pagerank equally among its outlinks
                double share = rank / outlinks.size();
                for(int temp : outlinks) 
                {
                    out.add(new Tuple2<>(temp, share));
                }
                return out.iterator();
            });

            if(!taxation)
            {
                //sum all contributions for each page & do not include dead ends
                ranks = value.filter(t -> t._1 != -1).reduceByKey(Double::sum);
            } 
            else
            {
                //taxation (beta)
                double beta = damping;

                //total rank or deadmass trapped in deadend pages
                double deadMass = value.filter(t -> t._1 == -1).values().fold(0.0, Double::sum);

                JavaPairRDD<Integer, Double> sum2 = value.filter(t -> t._1 != -1).reduceByKey(Double::sum);

                final double teleport = (1.0 - beta) / N;
                final double redistribution = deadMass / N;

                //combines values from linked pages, teleportation, and deadend redistribution.
                ranks = sum2.mapValues(sum -> (beta * sum) + teleport + (beta * redistribution)).union(links.keys().subtract(sum2.keys())
                              .mapToPair(k -> new Tuple2<>(k, teleport + (beta * redistribution))));
            }
        }

        return ranks;
    }

    public static void main(String[] args) throws IOException 
    {

        if(args.length < 4) 
        {
            System.err.println("Usage: PageRank <links-simple-sorted.txt> <titles-sorted.txt> <idealizedOut> <taxedOut>");
            System.exit(1);
        }

        String linksPath = args[0];
        String titlesPath = args[1];
        String outputIdeal = args[2];
        String outputTax = args[3];

        SparkConf conf = new SparkConf().setAppName("PA3-PageRank");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        //Load link/title data     
        Book.Doc doc = Book.mapPairs(sc, linksPath, titlesPath);
        JavaPairRDD<Integer, List<Integer>> links = doc.links;
        JavaPairRDD<Integer, String> titles = doc.titles;

        long N = links.count();
        int ITER = 25;

        //Ideal PageRank
        JavaPairRDD<Integer, Double> idealRanks = calculatePageRank(links, ITER, 1.0, false, N);

        JavaPairRDD<String, Double> idealOutput = idealRanks.join(titles).mapToPair(t -> new Tuple2<>(t._2._2, t._2._1)); //(title, rank)

        //sort by rank descending
        JavaRDD<Tuple2<String, Double>> idealSorted = idealOutput.map(t -> new Tuple2<>(t._1, t._2)).sortBy(t -> t._2, false, 1);

        //write file
        idealSorted.coalesce(1, true).map(t -> t._1 + "\t" + t._2).saveAsTextFile(outputIdeal);

        //important
        idealSorted.count();

        //Taxed PageRank (Basically the same as above but w Damping)
        JavaPairRDD<Integer, Double> taxedRanks = calculatePageRank(links, ITER, 0.85, true, N);

        JavaPairRDD<String, Double> taxedOutput = taxedRanks.join(titles).mapToPair(t -> new Tuple2<>(t._2._2, t._2._1));

        JavaRDD<Tuple2<String, Double>> taxedSorted = taxedOutput.map(t -> new Tuple2<>(t._1, t._2)).sortBy(t -> t._2, false, 1);

        taxedSorted.coalesce(1, true).map(t -> t._1 + "\t" + t._2).saveAsTextFile(outputTax);

        taxedSorted.count();
        sc.close();
    }
}
