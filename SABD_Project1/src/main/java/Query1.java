import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import utils.KeyFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static utils.CSVWriter.writeCSVFile;

public class Query1 {


    public static void main(String[] args) throws IOException {

        /**
         * INITIALIZATION
         */

        Properties prop = new Properties();


        FileInputStream f =null;
        if(args.length < 1){
            f = new FileInputStream("src/main/resources/configuration.properties");
        }
        else{
            if(args[0].equals("EMR")){
                f = new FileInputStream("src/main/resources/configurationemr.properties");
            }
            if(args[0].equals("local")){
                f = new FileInputStream("src/main/resources/configuration.properties");
            }
        }




        prop.load(f);

        String outputPath = prop.getProperty("QUERY1");

        String inputPath = prop.getProperty("NATIONAL_DATASET");

        /**
         * SET LOG LEVEL
         */

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        /**
         * FOR LOCAL CLUSTER RUNS
         */

        SparkConf conf;
        if(args[0].equals("EMR")){
            conf = new SparkConf().setAppName("Query 1");
        }else{
            conf = new SparkConf().setMaster("local").setAppName("Query 1");
        }


        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("EXECUTING QUERY 1");

        /**
         * GETTING START TIME
         */
        Instant start = Instant.now();

        final JavaRDD<String> nationalDataRaw = sc.textFile(inputPath,3);

        /**
         * CUTTING HEADER AND JUNK FROM DATASET
         */


        final String header = nationalDataRaw.first();

        JavaRDD<String> nationalData = nationalDataRaw.filter(x -> !x.equals(header)).cache();


        /**
         * GETTING VALUABLE INFO FROM NATIONAL RDD
         */

        JavaPairRDD<String, Tuple2<Double,Double>> nationalDataParsed = nationalData.mapToPair(
                (String line) ->  {

                    String[] slice = line.split(",");

                    return new Tuple2<String,Tuple2<Double,Double>>(
                            KeyFactory.getKey(slice[0].split("T")[0].replace("\"","")),
                            new Tuple2(Double.parseDouble(slice[9]),Double.parseDouble(slice[12])));

                });


        /**
         * CREATE RDD OF NATIONAL INFO ONE DAY OFF AND GET VALUABLE INFO FROM NATIONAL RDD ONE DAY OFF
         */

        String firstLine = nationalData.first();

        JavaPairRDD<String,Tuple2<Double,Double>> nationalDataDayOffParsed = nationalData.filter(x -> !x.equals(firstLine)).mapToPair(
                (String line) ->  {

                    String[] slice = line.split(",");


                    return new Tuple2<String,Tuple2<Double,Double>>(KeyFactory.getKeyMinusOne(slice[0].split("T")[0].replace("\"",""))
                            ,new Tuple2(Double.parseDouble(slice[9]),Double.parseDouble(slice[12])));

                });


        /**
         * JOIN THE RDDS TO COMPUTE DAILY DIFFERENCES
         */

        JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, Optional<Tuple2<Double, Double>>>> doubleNationalData = nationalDataParsed.leftOuterJoin(nationalDataDayOffParsed);

        /**
         * COMPUTE DAILY DIFFERENCES AND GROUP BY WEEK-BASED KEY
         */


        JavaPairRDD<Integer, Iterable<Tuple2<Double, Double>>> diffsWeeklyRDD = doubleNationalData.mapToPair(
                (Tuple2<String, Tuple2<Tuple2<Double, Double>, Optional<Tuple2<Double, Double>>>> t) -> {
            double diffDism = 0,diffSwab= 0;

            if(t._2._2.isPresent()){
                diffDism = t._2()._2().get()._1() - t._2()._1()._1();
                diffSwab = t._2()._2().get()._2() - t._2()._1()._2();
            }

            int week = KeyFactory.getWeeklyKey(t._1());



            return new Tuple2<Integer,Tuple2<Double,Double>>(week,new Tuple2<Double,Double>(diffDism,diffSwab));
        }).groupByKey();


        /**
         * COMPUTE RESULTS AND CREATE RDD OF RESULTS
         */

        JavaRDD<String> results = diffsWeeklyRDD.map((Tuple2<Integer, Iterable<Tuple2<Double, Double>>> t)->{

            double sumDism = 0,sumSwab = 0;
            int days = 0;
            for (Tuple2<Double,Double> tuple: t._2()
            ) {
                sumDism = sumDism + tuple._1();
                sumSwab = sumSwab + tuple._2();
                days++;
            }

            double avgDism = sumDism/(double) days;
            double avgSwab = sumSwab/(double) days;

            String week = String.valueOf(t._1());
            if(week.length() == 1){
                week = "0"+week;
            }

            String meanD = new DecimalFormat("##.##").format(avgDism );
            String meanS = new DecimalFormat("##.##").format(avgSwab );

            return week+ ","+ meanD.replace(",",".") +","
                    + meanS.replace(",",".");


            //return new Tuple2<Integer,Tuple2<Double,Double>>(t._1(),new Tuple2<Double,Double>(avgDism,avgSwab));
        }).sortBy(i -> i ,true,1);

        /**
         * GETTING END TIME BEFORE FILE SAVE
         */

        Instant end = Instant.now();
        results.saveAsTextFile(outputPath);
        /*

            WRITE FILE IN CSV FORMAT

            String[] newHeader = {"YearWeek","Mean dismissed","Mean Swabs"};
            writeCSVFile(results.collect(),"query1_resultsCSV.csv",newHeader);


         */


        /**
         * GETTING END TIME AFTER FILE SAVE
         */

        Instant endWithFile = Instant.now();

        System.out.println("EXECUTION TIME: " + Duration.between(start,end));
        System.out.println("EXECUTION TIME FILE SAVED : " + Duration.between(start,endWithFile));

        sc.stop();
    }

}
