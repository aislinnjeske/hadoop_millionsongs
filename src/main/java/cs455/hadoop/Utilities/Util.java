package cs455.hadoop.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class Util {

    public static int parseInt(String s){

        if(s.length() <= 1){
            return 0;
        }

        try {
            s = s.substring(1);

            int i = Integer.parseInt(s);

            return i;
        } catch(NumberFormatException e){
            return 0;
        }
    }

    public static double parseDouble(String s){

        if(s.length() <= 1){
            return 0.0;
        }

        try{
            s = s.substring(1);

            double d = Double.parseDouble(s);
            return d;
        } catch(NumberFormatException e){
            return 0.0;
        }
    }

    public static double parseSegment(String s){
        if(s.length() < 1){
            return 0.0;
        }

        try{
            double d = Double.parseDouble(s);
            return d;
        } catch(NumberFormatException e){
            return 0.0;
        }
    }

    public static ArrayList<String> calculateMostPopularTerms(Map<String, Integer> allTerms){
        Map<Integer, String> popTerms = new TreeMap<>(Collections.reverseOrder());
        ArrayList<String> mostPopularTerms = new ArrayList<>();

        for(String term : allTerms.keySet()){
            popTerms.put(allTerms.get(term), term);
        }

        int count = 0;
        for(Integer sum : popTerms.keySet()){

            if(count >= 10){
                break;
            }

            mostPopularTerms.add(popTerms.get(sum));
            count++;
        }

        return mostPopularTerms;
    }
}

