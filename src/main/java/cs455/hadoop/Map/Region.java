package cs455.hadoop.Map;

import cs455.hadoop.Song;
import cs455.hadoop.Utilities.Util;

import java.util.*;

public class Region {
    private ArrayList<Song> songs;
    public String regionName;

    public Region(String name){
        this.regionName = name;
        songs = new ArrayList<>();
    }

    public void add(Song s){
        songs.add(s);
    }

    public double calcAvgHotness(){
        double sum = 0;

        for(Song s : songs){
            sum += s.hotness;
        }
        return sum / songs.size();
    }

    public ArrayList<String> calcMostPopularSongs(){
        Double maxHotness = Double.MIN_VALUE;
        ArrayList<String> popularSongs = new ArrayList<>();

        for(Song s : songs){
            if(s.hotness > maxHotness){
                popularSongs.clear();
                popularSongs.add(s.songName);
            } else if(s.hotness.equals(maxHotness)){
                popularSongs.add(s.songName);
            }
        }
        return popularSongs;
    }

    public Map<String, Integer> calcTerms(){
        Map<String, Integer> terms = new HashMap<>();
        for(Song s : songs){

            for(int i = 0; i < s.artistTerms.length; i++){
                String term = s.artistTerms[i];

                if(terms.containsKey(term)){
                    int count = terms.remove(term);
                    count++;
                    terms.put(term, count);
                } else {
                    terms.put(term, 1);
                }

            }
        }
        return terms;
    }

    public ArrayList<String> getMostPopularTerms(){
        Map<String, Integer> terms = calcTerms();
        return Util.calculateMostPopularTerms(terms);
    }
}
