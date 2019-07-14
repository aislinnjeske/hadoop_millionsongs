package cs455.hadoop;

public class Song implements Comparable<Song>{

    public String songName;
    public String song_id;
    public String artistName;
    public Double duration;
    public Double hotness;
    public int timeSig;
    public int mode;
    public int key;
    public double fadeIn;
    public double fadeOut;
    public double tempo;
    public double loudness;
    public String[] artistTerms;
    public double lat;
    public double lon;

    public Song() {
        duration = new Double(0);
        hotness = new Double(0);
    }

    public int compareTo(Song other){
        if(duration.equals(other.duration)){
            return 0;
        } else if(this.duration > other.duration){
            return -1;
        } else {
            return 1;
        }
    }
}
