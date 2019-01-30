package com.kopykitab.analytic.als.train;

import com.kopykitab.analytic.als.model.Rating;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Encoders;

public class ALSTrainDataSet {
    public static void main(String[] args) {

        if (args.length < 4) {
            System.err.println(
                    "Usage: JavaALS <ratings_file> <no_of_recommendations> <iterations> <output_dir> [<blocks>]");
            System.exit(1);
        }

        String inputFile = args[0];
        //Constants
        int numberOfRecommendations = Integer.parseInt(args[1]);
        int rank = 10;
        int iterations = Integer.parseInt(args[2]);
        String outputDir = args[3];
        int blocks = -1;
        if (args.length == 5) {
            blocks = Integer.parseInt(args[4]);
        }

        SparkSession spark = SparkSession.builder().master("local[4]")
                .appName("KKRecommendationApp_DatasetApproch")
                .getOrCreate();

        JavaRDD<Rating> ratingsRDD = spark
                .read().textFile(inputFile).javaRDD()
                .map(Rating::parseRating);

        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        // Build the recommendation model using ALS on the training data
        ALS als = new ALS()
                .setMaxIter(iterations)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("bookId")
                .setRatingCol("rating");
        ALSModel model = als.fit(training);

        // Evaluate the model by computing the RMSE on the test data
        // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);

        // Generate top 10 book recommendations for each user
        //Dataset<Row> userRecs = model.recommendForAllUsers(10);

        // Generate top 10 user recommendations for each book
        //Dataset<Row> bookRecs = model.recommendForAllItems(10);

        // Generate top 10 book recommendations for a specified set of users
        Dataset<Row> users = ratings.select(als.getUserCol()).distinct();
        Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, numberOfRecommendations);
        System.out.println("Product Recommendation for users complete, writing output in " + outputDir);

        Dataset<String> convertToString = userSubsetRecs.map((MapFunction<Row,String>) row -> row.mkString(","), Encoders.STRING());
        convertToString.coalesce(1).write().csv(outputDir);

//        Dataframe df = userSubsetRecs.toDF();
//         org.apache.spark.sql.catalyst.plans.logical.Generate top 10 user recommendations for a specified set of books
//        Dataset<Row> books = ratings.select(als.getItemCol()).distinct().limit(3);
//        Dataset<Row> bookSubSetRecs = model.recommendForItemSubset(books, 10);
//        System.out.println("User Recommendation for book");
//        bookSubSetRecs.show(10);
//
//        spark.close();
    }
}
