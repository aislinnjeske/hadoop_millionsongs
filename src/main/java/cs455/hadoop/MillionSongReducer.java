package cs455.hadoop;

import cs455.hadoop.Utilities.*;
import cs455.hadoop.Map.WorldMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MillionSongReducer extends Reducer<Text, Text, Text, Text> {

    private Map<String, Artist> artistInfo = new HashMap<>();
    private ArrayList<Song> songs = new ArrayList<>();
    private ArrayList<Song> durations = new ArrayList<>();
    private ArrayList<Song> maxHottSong = new ArrayList<>();


    private ReadAttributes ra = new ReadAttributes();
    private GenericAverages ga = new GenericAverages();
    private AttributePredictor ap = new AttributePredictor();
    SegmentData sd = new SegmentData();
    private WorldMap map = new WorldMap();

    //[0] = num songs, [1] = loudest, [2] = total fading time
    String[] maxNames = {"Default", "Default", "Default"};
    double[] maxValues = {Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE};

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if(key.toString().equals("song_id")){
            return;
        } else {
            //Read and parse attributes
            ra.initialize(key, values, ap, ga, sd);
            ra.read();
            Song s = ra.getSong();

            //Adding the song to the duration list & world map & all songs list
            songs.add(s);
            map.add(s);

            if(!s.duration.equals(0)){
                durations.add(s);
            }

            //Updating artist information
            updateArtistMap(s, ra.getArtist_id());

            //Checking max hotness
            if(s.hotness.equals(1.0)){
                maxHottSong.add(s);
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        Collections.sort(songs);
        Collections.sort(durations);

        //Answering 8
        double[] genericAttributes = ga.getAverages();
        double[] maxDist = {Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE};
        double[] minDist = {Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE};

        String mostGeneric = "Default";
        String mostUnique = "Default";

        //Iterate through artists
        for(String artist : artistInfo.keySet()){
            Artist info = artistInfo.get(artist);

            if(hasMaxNumSongs(info)){
                maxValues[0] = info.numSongs;
                maxNames[0] = info.name;
            }

            if(hasMaxAvgLoudness(info)){
                maxValues[1] = info.sumLoudness;
                maxNames[1] = info.name;
            }

            if(hasMaxFadeTime(info)){
                maxValues[2] = info.sumFadeTime;
                maxNames[2] = info.name;
            }

            if(avgIsUnique(maxDist, genericAttributes, info)){
                mostUnique = info.name;
                setValues(maxDist, genericAttributes, info);
            } else if(avgIsGeneric(minDist, genericAttributes, info)){
                mostGeneric = info.name;
                setValues(minDist, genericAttributes, info);
            }
        }

        //Answering 5
        int medianIndex = durations.size() / 2;
        Double medianDuration = durations.get(medianIndex).duration;

        DurationComparer dc = new DurationComparer(medianDuration);

        //Iterate through all songs
        for(int i = 0; i < songs.size(); i++){
            Song s = songs.get(i);

            if(durations.contains(s)){
                dc.checkShortest(s);
                dc.checkLongest(s);
            }
        }


        //Calculate coefficients & map data
        ap.setMaxHotnessSongs(maxHottSong);
        ap.calculateNewValues();
        map.calcStats();

        //Write output

        //Question 1: Which artist has the most songs in the data set?
        context.write(new Text("Question One:"), new Text());
        context.write(new Text("Artist: " + maxNames[0]), new Text("Number of songs: " + Integer.toString((int) maxValues[0]) + '\n'));

        //Question 2: Which artist's songs are the loudest on average?
        context.write(new Text("Question Two:"), new Text());
        context.write(new Text("Artist: " + maxNames[1]), new Text("Avg Loudness: " + Double.toString(maxValues[1]) + '\n'));

        //Question 3: What's the song(s) with the highest hotness score?
        context.write(new Text("Question Three:"), new Text());
        for(Song song : maxHottSong){
            context.write(new Text("Song: "  + song.songName), new Text("Hotness: " + Double.toString(1.0)));
        }
        context.write(new Text(), new Text("\n"));

        //Question 4: Which artist has the highest total time spent fading in their songs?
        context.write(new Text("Question Four:"), new Text());
        context.write(new Text("Artist: "  + maxNames[2]), new Text("Total Fade Time: " + Double.toString(maxValues[2]) + '\n'));

        //Question 5: What is the longest song(s)? The shortest song(s)? The song(s) of median length?
        context.write(new Text("Question Five:"), new Text());
        context.write(new Text("Longest Song(s): "), new Text());
        for (String songName : dc.getLongestSongs()){
            context.write(new Text("Song: " + songName), new Text("Duration: " + Double.toString(dc.longestDuration)));
        }

        context.write(new Text("Shortest Song(s): "), new Text());
        for (String songName : dc.getShortestSongs()){
            context.write(new Text("Song: " + songName), new Text("Duration: " + Double.toString(dc.shortestDuration)));
        }

        context.write(new Text(), new Text("\n"));

        //Question 6: What are the 10 most energetic and danceable songs?
        context.write(new Text("Question Six:"), new Text());
        context.write(new Text("No Data Available"), new Text("\n"));

        //Question 7: Create segment data for the average song. (Start time, pitch, timbre, max loudness, max loudness time,
        // and start loudness)
        context.write(new Text("Question Seven:"), new Text());
        context.write(new Text("Start Time: "), new Text(sd.getAvgStartTime()));
        context.write(new Text("Pitch: "), new Text(sd.getAvgPitch()));
        context.write(new Text("Timbre: "), new Text(sd.getAvgTimbre()));
        context.write(new Text("Max Loudness: "), new Text(sd.getAvgMaxLoudness()));
        context.write(new Text("Max Loudness Time: "), new Text(sd.getAvgMaxLoudnessTime()));
        context.write(new Text("Start Loudness: "), new Text(sd.getAvgStartLoudness() + '\n'));

        //Question 8: Which artist is the most generic? Which artist is the most unique?
        context.write(new Text("Question Eight:"), new Text());
        context.write(new Text("Most Generic Artist: "), new Text(mostGeneric));
        context.write(new Text("Most Unique Artist: "), new Text(mostUnique));
        context.write(new Text(), new Text("\n"));

        //Question 9: Imagine a song with higher hotness score than Q3. (Tempo, time signature, duration, mode, key, loudness,
        //fade in, fade out, terms
        context.write(new Text("Question Nine:"), new Text());
        context.write(new Text("Title: "), new Text("Is It Summer Yet"));
        context.write(new Text("Artist: "), new Text("Aislinn Jeske"));
        context.write(new Text("Tempo: "), new Text(Double.toString(ap.getNewTempo())));
        context.write(new Text("Time Sig: "), new Text(Integer.toString(ap.getNewTimeSig())));
        context.write(new Text("Duration: "), new Text(Double.toString(ap.getNewDuration())));
        context.write(new Text("Mode: "), new Text(Integer.toString(ap.getNewMode())));
        context.write(new Text("Key: "), new Text(Integer.toString(ap.getNewKey())));
        context.write(new Text("Loudness: "), new Text(Double.toString(ap.getNewLoudness())));
        context.write(new Text("Fade In: "), new Text(Double.toString(ap.getNewFadeIn())));
        context.write(new Text("Fade Out: "), new Text(Double.toString(ap.getNewFadeOut())));
        context.write(new Text("Terms: "), new Text(ap.getNewTerms() + "\n"));

        //Question 10: What are the most popular songs, average hotness, and popular genres in the world by region?
        context.write(new Text("Question Ten:"), new Text());
        context.write(new Text("Region; Average Hotness per Region; Popular Songs per Region; Terms per Region"), new Text());
        context.write(new Text("North America:"), new Text(map.getResultsByRegion(0)));
        context.write(new Text("Central America:"), new Text(map.getResultsByRegion(1)));
        context.write(new Text("South America:"), new Text(map.getResultsByRegion(2)));
        context.write(new Text("Western Europe:"), new Text(map.getResultsByRegion(3)));
        context.write(new Text("Africa & Middle East:"), new Text(map.getResultsByRegion(4)));
        context.write(new Text("Asia, Eastern Europe, & Australia:"), new Text(map.getResultsByRegion(5)));
    }

    public void updateArtistMap(Song s, String artist_id){

        //Updating artist counts/sums
        if(!artistInfo.containsKey(artist_id)){
            Artist a = new Artist(1, s.loudness, (s.duration - s.fadeOut) + s.fadeIn, s.duration, s.hotness, s.tempo, s.artistName);
            artistInfo.put(artist_id, a);
        } else {
            Artist a = artistInfo.remove(artist_id);
            a.numSongs += 1;
            a.addLoudness(s.loudness);
            a.addDuration(s.duration);
            a.addFadeTime((s.duration - s.fadeOut) + s.fadeIn);
            a.addHotness(s.hotness);
            a.addTempo(s.tempo);
            artistInfo.put(artist_id, a);
        }
    }

    public boolean hasMaxNumSongs(Artist a){
        return a.numSongs > maxValues[0];
    }

    public boolean hasMaxAvgLoudness(Artist a){
        return a.getAvgLoudness() > maxValues[1];
    }

    public boolean hasMaxFadeTime(Artist a){
        return a.sumFadeTime > maxValues[2];
    }

    public boolean avgIsUnique(double[] maxDistances, double[] genericAttributes, Artist a){
        //[0] = hotness, [1] = duration, [2] = loudness, [3] = tempo
        boolean maxH = Math.abs((a.sumHotness / a.numSongs) - genericAttributes[0]) > maxDistances[0];
        boolean maxD = Math.abs((a.sumDuration / a.numSongs) - genericAttributes[1]) > maxDistances[1];
        boolean maxL = Math.abs((a.sumLoudness / a.numSongs) - genericAttributes[2]) > maxDistances[2];
        boolean maxT = Math.abs((a.sumTempo / a.numSongs) - genericAttributes[3]) > maxDistances[3];

        return maxH && maxD && maxL && maxT;
    }

    public void setValues(double[] dist, double[] generic, Artist a){
        dist[0] = Math.abs((a.sumHotness / a.numSongs) - generic[0]);
        dist[1] = Math.abs((a.sumDuration / a.numSongs) - generic[1]);
        dist[2] = Math.abs((a.sumLoudness / a.numSongs) - generic[2]);
        dist[3] = Math.abs((a.sumTempo / a.numSongs) - generic[3]);
    }

    public boolean avgIsGeneric(double[] minDist, double[] genericAttributes, Artist a){
        //[0] = hotness, [1] = duration, [2] = loudness, [3] = tempo
        boolean minH = Math.abs((a.sumHotness / a.numSongs) - genericAttributes[0]) < minDist[0];
        boolean minD = Math.abs((a.sumDuration / a.numSongs) - genericAttributes[1]) < minDist[1];
        boolean minL = Math.abs((a.sumLoudness / a.numSongs) - genericAttributes[2]) < minDist[2];
        boolean minT = Math.abs((a.sumTempo / a.numSongs) - genericAttributes[3]) < minDist[3];

        return minH && minD && minL && minT;
    }
}
