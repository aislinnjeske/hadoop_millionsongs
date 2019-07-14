package cs455.hadoop.Map;

import cs455.hadoop.Song;

import java.util.ArrayList;

public class WorldMap {

    private ArrayList<Region> map;
    private ArrayList<ArrayList<String>> popSongsByRegion;
    private ArrayList<ArrayList<String>> termsByRegion;
    private double[] hotnessByRegion;

    public WorldMap(){
        map = new ArrayList<>();
        map.add(new Region("North America"));
        map.add(new Region("Central America"));
        map.add(new Region("South America"));
        map.add(new Region("Western Europe"));
        map.add(new Region("Africa & Middle East"));
        map.add(new Region("Asia, Eastern Europe, & Australia"));

        popSongsByRegion = new ArrayList<>();
        termsByRegion = new ArrayList<>();
        hotnessByRegion = new double[6];
    }

    public void add(Song s){
        double lat = s.lat;
        double lon = s.lon;

        if(lat >= 28 && lat <= 90 && lon >= -180 && lon <= -60){
            //North America
            map.get(0).add(s);
        } else if(lat >= 8 && lat <= 27 && lon >= -180 && lon <= -60){
            //Central America
            map.get(1).add(s);
        } else if(lat >= -60 && lat <= 7 && lon >= -180 && lon <= -30){
            //South America
            map.get(2).add(s);
        } else if(lat >= 35 && lat <= 90 && lon >= -10 && lon <= 40){
            //Western Europe
            map.get(3).add(s);
        } else if(lat >= -38 && lat <= 35 && lon >= -20 && lon <= 40){
            //Africa & ME
            map.get(4).add(s);
        } else if(lat >= -35 && lat <= 90 && lon >= 40 && lon <= 180){
            //Asia, Eastern Europe, Australia
            map.get(5).add(s);
        }
    }

    public void calcHotnessByRegion(){
        int index = 0;

        for(Region r : map){
            hotnessByRegion[index] = Math.round(r.calcAvgHotness() * 100.0) / 100.0;
            index++;
        }
    }

    public void calcPopularSongsByRegion(){
        for(Region r : map){
            popSongsByRegion.add(r.calcMostPopularSongs());
        }
    }

    public void calcTermsByRegion(){
        for(Region r : map){
            termsByRegion.add(r.getMostPopularTerms());
        }
    }

    public void calcStats(){
        calcHotnessByRegion();
        calcPopularSongsByRegion();
        calcTermsByRegion();
    }

    public String getResultsByRegion(int region){
        return "Hotness: " + Double.toString(hotnessByRegion[region]) + "; " + "Song: " + popSongsByRegion.get(region).get(0) + "; " + "Terms: " + termsByRegion.get(region).toString();
    }

}
