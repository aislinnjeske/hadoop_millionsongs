package cs455.hadoop.Utilities;

public class SegmentData {

    private double[] startTime;
    private double[] pitch;
    private double[] timbre;
    private double[] maxLoudness;
    private double[] maxLoudnessTime;
    private double[] startLoudness;
    private int numSongs;

    public SegmentData(){
        startTime = new double[10];
        pitch = new double[12];
        timbre = new double[12];
        maxLoudness = new double[10];
        maxLoudnessTime = new double[10];
        startLoudness = new double[10];
        this.numSongs = 0;
    }

    public void addSong(){
        numSongs++;
    }

    public void addStartTime(double[] st){
        for(int i = 0; i < st.length; i++){
            startTime[i] += st[i];
        }
    }

    public void addPitch(double[] p){
        for(int i = 0; i < p.length; i++){
            pitch[i] += p[i];
        }
    }

    public void addTimbre(double[] t){
        for(int i = 0; i < t.length; i++){
            timbre[i] += t[i];
        }
    }

    public void addMaxLoudness(double[] ml){
        for(int i = 0; i < ml.length; i++){
            maxLoudness[i] += ml[i];
        }
    }

    public void addMaxLoudnessTime(double[] mlt){
        for(int i = 0; i < mlt.length; i++){
            maxLoudnessTime[i] += mlt[i];
        }
    }

    public void addStartLoudness(double[] sl){
        for(int i = 0; i < sl.length; i++){
            startLoudness[i] += sl[i];
        }
    }

    public String getAvgStartTime(){
        String s = "";

        for(int i = 0; i < startTime.length - 1; i++){
            s += startTime[i] / numSongs + ", ";
        }
        s += startTime[startTime.length -1] / numSongs;


        return s;
    }

    public String getAvgPitch(){
        String s = "";

        for(int i = 0; i < pitch.length - 1; i++){
            s += pitch[i] / numSongs + ", ";
        }
        s += pitch[pitch.length - 1] / numSongs;

        return s;
    }

    public String getAvgTimbre(){
        String s = "";

        for(int i = 0; i < timbre.length - 1; i++){
            s += timbre[i] / numSongs + ", ";
        }
        s += timbre[timbre.length -1] / numSongs;

        return s;
    }

    public String getAvgMaxLoudness(){
        String s = "";

        for(int i = 0; i < maxLoudness.length - 1; i++){
            s += maxLoudness[i] / numSongs + ", ";
        }
        s += maxLoudness[maxLoudness.length -1] / numSongs;

        return s;
    }

    public String getAvgMaxLoudnessTime(){
        String s = "";

        for(int i = 0; i < maxLoudnessTime.length - 1; i++){
            s += maxLoudnessTime[i] / numSongs + ", ";
        }
        s += maxLoudnessTime[maxLoudnessTime.length -1] / numSongs;

        return s;
    }

    public String getAvgStartLoudness(){
        String s = "";

        for(int i = 0; i < startLoudness.length - 1; i++){
            s += startLoudness[i] / numSongs + ", ";
        }
        s += startLoudness[startLoudness.length -1] / numSongs;


        return s;
    }
}
