package cs455.hadoop.Utilities;

public class GenericAverages {

    private double sumHotness;
    private int countHotness;
    private double sumDuration;
    private int countDuration;
    private double sumLoudness;
    private int countLoudness;
    private double sumTempo;
    private int countTempo;
    private double sumSegmentMaxLoudness;
    private int countSegment;

    public GenericAverages(){
        sumHotness = sumDuration = sumLoudness = sumTempo = countHotness = countDuration = countLoudness = countTempo = countSegment = 0;
        sumSegmentMaxLoudness = 0;
    }

    public void addHotness(double hotness){
        sumHotness += hotness;
        countHotness++;
    }

    public void addDuration(double duration){
        sumDuration += duration;
        countDuration++;
    }

    public void addLoudness(double loudness){
        sumLoudness += loudness;
        countLoudness++;
    }

    public void addTempo(double tempo){
        sumTempo += tempo;
        countTempo++;
    }

    public void addSegments(double segmentAvg){
        sumSegmentMaxLoudness += segmentAvg;
        countSegment++;
    }

    public double[] getAverages(){
        double[] avgs = {sumHotness / countHotness, sumDuration / countDuration, sumLoudness / countLoudness, sumTempo / countTempo, sumSegmentMaxLoudness / 1};
        return avgs;
    }
}
