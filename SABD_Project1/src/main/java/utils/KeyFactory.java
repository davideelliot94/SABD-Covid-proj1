package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class KeyFactory {



    public static String getKey(String s) throws ParseException {

        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd");

        Date date = sdf.parse(s);

        return sdf.format(date);

    };

    public static String getKeyMinusOne(String s) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Date date = sdf.parse(s);

        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, -1);
        Date dt = c.getTime();

        String finalDate = null;

        finalDate = sdf.format(dt);

        return finalDate;

    };

    public static int getWeeklyKey(String s) throws ParseException {



        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(s);

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        return week;
    }

    public static int getWeek(String s) throws ParseException {


        String[] slice = s.split("/");
        String ns = slice[2] + slice[2] +"-"+slice[0]+"-"+slice[1];

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(ns);

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        return week;
    }

}
