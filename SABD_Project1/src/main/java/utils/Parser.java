package utils;
import org.apache.commons.lang.StringUtils;
import scala.Tuple2;
import scala.Tuple3;
import java.text.ParseException;
import java.util.*;

public class Parser {

    public static Tuple3<String,double[],Double> parseLineSlope(String line) {



        String[] l = line.split(",");


        String location = "";

        if(l.length < 4){
            return new Tuple3<String,double[],Double>(line,null,null);
        }
        double[] y = new double[l.length-4];
        double[] x = new double[l.length-4];


        int i = 4;

        while( i < l.length ){
            if(!StringUtils.isNumeric(l[i-4]) && i-4 <= 1){
                if(l[i-4].isEmpty()) {
                    location = location;
                }
                if(l[i-4].contains("\"")){
                    location = location +","+ l[i-4].replace("\"","");
                }
                else{
                    location = location +","+ l[i-4];
                }
            }

            y[i-4] = Double.parseDouble(l[i]);
            x[i-4] = (double) i-4;
            i++;

        }

        location = location.substring(1);


        double slope = TrendlineCoefficientComputer.computeCoefficient(x,y);

        return new Tuple3<String,double[],Double>(location,y,slope);

    }

    public static String getContinent(String country, List<Tuple2<String, String>> continents) {

        int j = 0;

        String gotContinent = "";

        while (j < continents.size()) {
            Locale l = new Locale("", continents.get(j)._1);

            if (country.equals("Czechia")) country = "Czech Republic";
            if (country.equals("North Macedonia")) country = "MK";
            if (country.equals("Cote d'Ivoire")) country = "CI";

            if(country.contains(",")){
                country = country.split("")[1];
            }
            if (continents.get(j)._1.equals(country) ||
                    l.getDisplayCountry(Locale.ENGLISH).equals(country) ||
                    l.getDisplayCountry(Locale.ENGLISH).contains(country)) {

                gotContinent = continents.get(j)._2;

                //System.out.println("GOT: "+ gotContinent+ "," + l.getDisplayCountry());

                break;
            } else {
                j++;
            }
        }

        String res = "";
        switch (gotContinent) {
            case "NA":
            case "SA":
                res = "AMERICA";
                break;
            case "AS":
                res = "ASIA";
                break;
            case "EU":
                res = "EUROPA";
                break;
            case "OC":
                res = "OCEANIA";
                break;
            case "AN":
                res = "ANTARTIDE";
                break;
            case "AF":
                res = "AFRICA";
                break;

        }
        return res;
    }


    public static Vector<Tuple2<String, Tuple2<List<Double>, Integer>>> getWeeklyInfo(List<Tuple2<String,double[]>> collect, String header)
            throws ParseException {

        Vector<Tuple2<String,Tuple2<List<Double>,Integer>>> list = new Vector<>();

        String[] l = header.split(",");

        int sizeHeader = l.length;
        int prevWeek = KeyFactory.getWeek(l[4]);

        for (Tuple2<String,double[]> t: collect
             ) {
            int i = 4;
            List<Double> d  = new ArrayList<>();
            int week;

            while(i < sizeHeader){
                week = KeyFactory.getWeek(l[i]);

                if (week != prevWeek) {
                    list.add(new Tuple2<>(t._1(), new Tuple2(d, prevWeek)));
                    d = new ArrayList<>();
                }
                d.add(t._2()[i-4]);
                prevWeek = week;
                i++;
            }
            list.add(new Tuple2<>(t._1(), new Tuple2(d, prevWeek)));

        }
        return list;
    }


    /**
     * END PARSER
     */
}

