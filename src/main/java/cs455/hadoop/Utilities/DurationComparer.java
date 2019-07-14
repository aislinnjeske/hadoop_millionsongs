package cs455.hadoop.Utilities;

import cs455.hadoop.Song;

import java.util.ArrayList;

public class DurationComparer {
    private ArrayList<String> longestSongs;
    private ArrayList<String> shortestSongs;
    public Double shortestDuration;
    public Double longestDuration;

    public DurationComparer(Double medianDuration){
        this.longestSongs = new ArrayList<>();
        this.shortestSongs = new ArrayList<>();
        this.shortestDuration = Double.MAX_VALUE;
        this.longestDuration = Double.MIN_VALUE;
    }

    public void checkShortest(Song s){
        if(s.duration < shortestDuration && s.duration != 0) {
            shortestSongs.clear();
            shortestSongs.add(s.songName);
            shortestDuration = s.duration;
        } else if(s.duration.equals(shortestDuration)){
            shortestSongs.add(s.songName);
        }
    }

    public void checkLongest(Song s){
        if(s.duration > longestDuration && s.duration != 0){
            longestSongs.clear();
            longestSongs.add(s.songName);
            longestDuration = s.duration;
        } else if(s.duration.equals(longestDuration)) {
            longestSongs.add(s.songName);
        }
    }

    public ArrayList<String> getLongestSongs(){
        return longestSongs;
    }

    public ArrayList<String> getShortestSongs() {
        return shortestSongs;
    }
}
