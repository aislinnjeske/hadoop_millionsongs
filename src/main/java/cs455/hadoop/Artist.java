package cs455.hadoop;

public class Artist {

    public String name;
    public int numSongs;
    public double sumLoudness;
    public double sumFadeTime;
    public double sumDuration;
    public double sumHotness;
    public double sumTempo;

    public Artist(int numSongs, double sumLoudness, double sumFadeTime, double sumDuration, double sumHotness, double sumTempo, String name){
        this.name = name;
        this.numSongs = numSongs;
        this.sumLoudness = sumLoudness;
        this.sumFadeTime = sumFadeTime;
        this.sumDuration = sumDuration;
        this.sumHotness = sumHotness;
        this.sumTempo = sumTempo;
    }

    public void addLoudness(double loudness){
        if(loudness != 0){
            sumLoudness += loudness;
        }
    }

    public void addFadeTime(double fadeTime){
        if(fadeTime != 0){
            sumFadeTime += fadeTime;
        }
    }

    public void addDuration(double duration){
        if(duration != 0){
            sumDuration += duration;
        }
    }

    public void addHotness(double hotness){
        if(hotness != 0){
            sumHotness += hotness;
        }
    }

    public void addTempo(double tempo){
        if(tempo != 0){
            sumTempo += tempo;
        }
    }

    public double getAvgLoudness(){
        return (sumLoudness / numSongs);
    }
}
