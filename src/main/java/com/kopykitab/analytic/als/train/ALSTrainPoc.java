package com.kopykitab.analytic.als.train;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import java.util.Arrays;
import java.util.regex.Pattern;
import scala.Tuple2;

public class ALSTrainPoc {

    public static void main(String[] args) {

        if (args.length < 4) {
            System.err.println(
                    "Usage: JavaALS <ratings_file> <rank> <iterations> <output_dir> [<blocks>]");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("JavaALS");

        //Constants
        int rank = Integer.parseInt(args[1]);
        int iterations = Integer.parseInt(args[2]);
        String outputDir = args[3];
        int blocks = -1;
        if (args.length == 5) {
            blocks = Integer.parseInt(args[4]);
        }

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<Rating> ratings = lines.map(new ParseRating());

        MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks);


        //ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks).recommendProductsForUsers(5).toJavaRDD().collect();

        /* Can Save the trained model for future use.
        model.save(sparkContext,"");
        */

        model.recommendProductsForUsers(5).toJavaRDD().map(new com.kopykitab.analytic.als.train.ALSTrainPoc.FeaturesToRatingString()).coalesce(1).saveAsTextFile(outputDir + "/recommendation");
        System.out.println("Final user/product features written to " + outputDir);
        sc.stop();
    }
    
    static class ParseRating implements Function<String, Rating> {
        private static final Pattern COMMA = java.util.regex.Pattern.compile(",");

        @Override
        public Rating call(String line) {
            String[] tok = COMMA.split(line);
            int x = Integer.parseInt(tok[0]);
            int y = Integer.parseInt(tok[1]);
            double rating = Double.parseDouble(tok[2]);
            return new Rating(x, y, rating);
        }
    }

    static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
        @Override
        public String call(Tuple2<Object, double[]> element) {
            return element._1() + "," + Arrays.toString(element._2());
        }
    }

    static class FeaturesToRatingString implements Function<Tuple2<Object, Rating[]>, String> {

        @Override
        public String call(Tuple2<Object, Rating[]> element) {
            String user = element._1.toString();
            Rating[] rating = element._2();
            String recommendedProducts = "";

            for(Rating r : rating){
               // if(!recommendedProducts.isEmpty())
                recommendedProducts = recommendedProducts + "," + r.product();
            }
         return user + "," + recommendedProducts;
        }
    }
}
