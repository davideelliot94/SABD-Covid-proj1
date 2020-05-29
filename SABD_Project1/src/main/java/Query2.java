import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.Parser;
import utils.StatisticsComputer;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static utils.CSVWriter.writeCSVFile;


public class Query2 {

    public static void main(String[] args) throws IOException, ParseException {

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

        String outputPath = prop.getProperty("QUERY2");
        String inputPath = prop.getProperty("GLOBAL_DATASET");
        String cPath = prop.getProperty("CONTINENTS");

        /**
         * SET LOG LEVEL
         */

        Logger.getLogger("org.apache").setLevel(Level.WARN);


        /**
         * FOR EMR CLUSTER RUNS
        */

        SparkConf conf;
        if(args[0].equals("EMR")){
            conf = new SparkConf().setAppName("Query 2");
        }else{
            conf = new SparkConf().setMaster("local").setAppName("Query 2");
        }




        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("EXECUTING QUERY 2");


        /**
         * GETTING START TIME
         */


        Instant start = Instant.now();  


        /**
         * LOAD DATASET AS STRING RDD
         */

        final JavaRDD<String> timeSeriesDataset = sc.textFile(inputPath,6);

        final String header = timeSeriesDataset.first();



        /**
         * REMOVING HEADER
         */


        JavaRDD<String> timeSeriesNoHeader = timeSeriesDataset.filter(x -> !x.equals(header));

        /**
         * LOAD CONTINENTS FILE AS RDD
         */

        JavaRDD<String> continentsRaw = sc.textFile(cPath,0);

        final String head = continentsRaw.first();

        final List<Tuple2<String,String>> continents = continentsRaw.filter(x -> !x.equals(head)).mapToPair(line -> {

            String[] slice = line.split(",");

            return new Tuple2<String,String>(slice[0],slice[1]);
        }).collect();

        /**
         *  CREATE RDD COMPUTING TRENDLINE COEFFICIENT FOR EACH COUNTRY
         *
         *  SORT RDD BY TRENDLINE COEFFICIENT IN DESCENDING ORDER
         *
         */


        JavaRDD<Tuple3<String,double[],Double>> timeSeriesTrendSorted = timeSeriesNoHeader.map(Parser::parseLineSlope)
                .filter(x -> x._3()!=null)
                .sortBy(Tuple3::_3,false,1);



        /**
         * TAKE FIRST 100 ROWS FROM THE RDD THAT WILL BE THE MOST AFFECTED COUNTRIES
         * MAP THE TIME SERIES RDD IN PAIRS USING COUNTRY AS KEY
         */



        JavaPairRDD<String,double[]> timeSeriesTrendReduced = sc.parallelize(timeSeriesTrendSorted.take(100))
                .mapToPair((Tuple3<String,double[],Double> t) -> {
                    return new Tuple2<String,double[]>(t._1(),t._2());
                }).cache();


        /**
         * MAKING 2 PAIR RDD:
         *  1) KEY,VALUE -> COUNTRY, CONTINENT
         *  2) KEY,VALUE -> COUNTRY, Tuple2<ARRAY OF DATA, WEEK>
         */

        JavaPairRDD<String,String> countryContinentRDD = timeSeriesTrendReduced.mapToPair((Tuple2<String,double[]> t) -> {
            String country = t._1();
            String continent = Parser.getContinent(country,continents);
            return new Tuple2<>(country,continent);
        });


        /**
         * USE COLLECT BECAUSE THE RDD IS SMALL ENOUGH NOT TO BURDEN ON THE DRIVER PROGRAM
         * DIVIDE THE COLLECTION IN PARTITIONS TO MAKE THE SPARK TASKS LIGHTWEIGHT AND THAN PERFORM AN UNION
         */


        List<Tuple2<String, double[]>> tmp = timeSeriesTrendReduced.collect();


        JavaPairRDD<String,Tuple2<List<Double>,Integer>> countryWeeksRDD1 = sc.parallelizePairs(Parser.getWeeklyInfo(tmp.subList(0,15),header));
        JavaPairRDD<String,Tuple2<List<Double>,Integer>> countryWeeksRDD2 = sc.parallelizePairs(Parser.getWeeklyInfo(tmp.subList(15,30),header));
        JavaPairRDD<String,Tuple2<List<Double>,Integer>> countryWeeksRDD3 = sc.parallelizePairs(Parser.getWeeklyInfo(tmp.subList(30,45),header));
        JavaPairRDD<String,Tuple2<List<Double>,Integer>> countryWeeksRDD4 = sc.parallelizePairs(Parser.getWeeklyInfo(tmp.subList(45,60),header));
        JavaPairRDD<String,Tuple2<List<Double>,Integer>> countryWeeksRDD5 = sc.parallelizePairs(Parser.getWeeklyInfo(tmp.subList(60,75),header));
        JavaPairRDD<String,Tuple2<List<Double>,Integer>> countryWeeksRDD6 = sc.parallelizePairs(Parser.getWeeklyInfo(tmp.subList(75,90),header));
        JavaPairRDD<String,Tuple2<List<Double>,Integer>> countryWeeksRDD7 = sc.parallelizePairs(Parser.getWeeklyInfo(tmp.subList(90,100),header));


        JavaPairRDD<String,Tuple2<List<Double>,Integer>> countryWeeksRDD = sc.union(countryWeeksRDD1,countryWeeksRDD2,
                countryWeeksRDD3,countryWeeksRDD4,countryWeeksRDD5,countryWeeksRDD6,countryWeeksRDD7);


        /**
         * JOIN ON THE KEY FIELD OF THE 2 PREVIOUSLY CREATED AND MAP TO PAIRS < YEAR WEEK,CONTINENT >
         */

        JavaPairRDD<String, Tuple2<String, Tuple2<List<Double>, Integer>>> joinedRDD = countryContinentRDD.join(countryWeeksRDD);


        JavaPairRDD<Tuple2<Integer, String>, List<Double>> rddByWeekRaw = joinedRDD.mapToPair(
                (Tuple2<String, Tuple2<String, Tuple2<List<Double>, Integer>>> t) ->{
                    return new Tuple2<>(new Tuple2<Integer, String>(t._2()._2()._2(),t._2()._1()),t._2()._2()._1());
                }
        );

        /**
         * CREATE A NEW PAIR RDD:
         *
         *  KEY -> Tuple2< CONTINENT,WEEK >
         *
         *  PERFORM A REDUCEBYKEY TO GROUP ALL ARRAYS OF DATA RELATIVE TO A CERTAIN WEEK AND CONTINENT
         */

        JavaPairRDD<Tuple2<Integer, String>, List<Double>> rddByWeek = rddByWeekRaw.reduceByKey(
                (x,y) -> Stream.concat(x.stream(),y.stream()).collect(Collectors.toList())
        ).cache();


        /**
         * CREATE COMPUTED VALUES RDD
         */


        JavaPairRDD<Tuple2<Integer,String>, Tuple4<Double,Double,Double,Double>> resultsRaw = rddByWeek.mapToPair(
                (Tuple2<Tuple2<Integer, String>, List<Double>> t) ->{


                    double mean = StatisticsComputer.mean(t._2(),t._2().size());
                    double var = Math.sqrt(StatisticsComputer.variance(t._2(),t._2().size(),mean));
                    double min = StatisticsComputer.getMin(t._2(),t._2().size());
                    double max = StatisticsComputer.getMax(t._2(),t._2().size());
                    return new Tuple2<Tuple2<Integer,String>,Tuple4<Double,Double,Double,Double>>(
                            t._1(),
                            new Tuple4<Double,Double,Double,Double>(mean,var,min,max));


                }
        );


        /**
         * PREPARE STRING RDD TO SAVE RESULTS SORTED LEXICOGRAPHICALLY
         */

        JavaRDD<String> results = resultsRaw.map(
                (Tuple2<Tuple2<Integer, String>, Tuple4<Double, Double, Double, Double>> t) -> {

                    String mean = new DecimalFormat("##.##").format(t._2()._1());
                    String var = new DecimalFormat("##.##").format(t._2()._2());
                    String min = new DecimalFormat("##.##").format(t._2()._3());
                    String max = new DecimalFormat("##.##").format(t._2()._4());

                    String week = String.valueOf(t._1()._1());
                    if(week.length()==1){
                        week = "0"+week;
                    }

                    return week +","+
                            t._1()._2() + ","+
                            mean.replace(",",".") + ","+
                            var.replace(",",".") + ","+
                            min.replace(",",".")+ ","+
                            max.replace(",",".");

                }
        ).sortBy(i ->i ,true,1);


        /**
         * GETTING END COMPUTATION TIME
         */

        Instant end = Instant.now();

        results.coalesce(1,false).saveAsTextFile(outputPath);


        /*

            WRITE FILE IN CSV FORMAT

            String[] newHeader = {"YearWeek","Continent","Mean","Standard Deviation","min","Max"};
            writeCSVFile(results.collect();,"query2_resultsCSV.csv",newHeader);


         */



        /**
         * GETTING END COMPUTATION TIME WITH FILE SAVED
         */

        Instant endWithFile = Instant.now();

        System.out.println("EXECUTION TIME: " + Duration.between(start,end));
        System.out.println("EXECUTION TIME saved file: " + Duration.between(start,endWithFile));


        sc.stop();
    }

}
