package cs455.hadoop.Utilities;

import cs455.hadoop.Song;
import org.apache.hadoop.io.Text;

public class ReadAttributes {

    private Iterable<Text> values;
    private Text key;
    private Song song;
    private String artist_id;
    private AttributePredictor ap;
    private GenericAverages ga;
    private SegmentData sd;

    public void initialize(Text key, Iterable<Text> values, AttributePredictor ap, GenericAverages ga, SegmentData sd){
        this.key = key;
        this.values = values;
        this.ap = ap;
        song = new Song();
        this.ga = ga;
        this.sd = sd;
    }

    public void read(){

        //Getting values and creating new song object
        for(Text value : values){
            if(isAnalysis(value)){

                String[] songInfo = splitText(value.toString(), "@");

                song.hotness = Util.parseDouble(songInfo[1]);
                ga.addHotness(song.hotness);

                song.duration = Util.parseDouble(songInfo[2]);
                ap.addValue(song.duration, song.hotness, "duration");
                ga.addDuration(song.duration);

                song.loudness = Util.parseDouble(songInfo[5]);
                ap.addValue(song.loudness, song.hotness, "loudness");
                ga.addLoudness(song.loudness);

                song.tempo = Util.parseDouble(songInfo[8]);
                ap.addValue(song.tempo, song.hotness, "tempo");
                ga.addTempo(song.tempo);

                song.fadeIn = Util.parseDouble(songInfo[3]);
                ap.addValue(song.fadeIn, song.hotness,"fadeIn");

                song.key = Util.parseInt(songInfo[4]);
                ap.addValue(song.key, song.hotness,"key");

                song.mode = Util.parseInt(songInfo[6]);
                ap.addValue(song.mode, song.hotness, "mode");

                song.fadeOut = Util.parseDouble(songInfo[7]);
                ap.addValue(song.fadeOut, song.hotness, "fadeOut");

                song.timeSig = Util.parseInt(songInfo[9]);
                ap.addValue(song.timeSig, song.hotness, "timeSig");

                sd.addMaxLoudness(stringToDouble(splitText(songInfo[13].substring(1), " ")));

                sd.addStartTime(stringToDouble(splitText(songInfo[10].substring(1), " ")));

                sd.addPitch(stringToDouble(splitText(songInfo[11].substring(1), " ")));

                sd.addTimbre(stringToDouble(splitText(songInfo[12].substring(1), " ")));

                sd.addMaxLoudnessTime(stringToDouble(splitText(songInfo[14].substring(1), " ")));

                sd.addStartLoudness(stringToDouble(splitText(songInfo[15].substring(1), " ")));

                sd.addSong();

            } else {
                String[] singerInfo = splitText(value.toString(), "%");

                song.songName = singerInfo[1];
                song.song_id = key.toString();

                artist_id = singerInfo[2];
                song.artistName = singerInfo[3];

                song.lat = Util.parseDouble(singerInfo[4]);
                song.lon = Util.parseDouble(singerInfo[5]);

                song.artistTerms = splitText(singerInfo[6], " ");
                ap.addTerms(song.artistTerms);
            }
        }
    }

    public boolean isAnalysis(Text text){
        return text.toString().charAt(0) == '@';
    }

    public String[] splitText(String s, String delimitor){
        return s.split(delimitor);
    }

    public double[] stringToDouble(String[] s){
        double[] d = new double[s.length];

        for(int i = 0; i < s.length; i++){
            d[i] = Util.parseSegment(s[i]);
        }

        return d;
    }

    public Song getSong(){
        return song;
    }

    public String getArtist_id(){
        return artist_id;
    }

}
