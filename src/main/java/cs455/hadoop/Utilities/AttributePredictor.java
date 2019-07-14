package cs455.hadoop.Utilities;

import cs455.hadoop.Song;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AttributePredictor {

    private ArrayList<Coefficient> data;
    private Map<String, Integer> allTerms;
    private ArrayList<String> mostPopTerms;
    private double[] coefficients;
    private double[] maxHotnessAverages;
    private double[] newValues;

    public AttributePredictor(){
        data = new ArrayList<>();
        for(int i = 0; i < 8; i++){
            data.add(new Coefficient());
        }

        mostPopTerms = new ArrayList<>();
        allTerms = new HashMap<>();
        coefficients = new double[8];
        maxHotnessAverages = new double[8];
        newValues = new double[8];
    }

    public void addValue(double value, double hotness, String identifier){
        switch(identifier){
            case "tempo":
                data.get(0).add(value, hotness);
                break;
            case "timeSig":
                data.get(1).add(value, hotness);
                break;
            case "duration":
                data.get(2).add(value, hotness);
                break;
            case "mode":
                data.get(3).add(value, hotness);
                break;
            case "loudness":
                data.get(4).add(value, hotness);
                break;
            case "key":
                data.get(5).add(value, hotness);
                break;
            case "fadeIn":
                data.get(6).add(value, hotness);
                break;
            case "fadeOut":
                data.get(7).add(value, hotness);
                break;
        }
    }

    public void addTerms(String[] terms){
        for(int i = 0; i < terms.length; i++){
            String t = terms[i];

            if(allTerms.containsKey(t)){
                int count = allTerms.remove(t);
                count++;
                allTerms.put(t, count);
            } else {
                allTerms.put(t, 1);
            }
        }
    }

    public void setMaxHotnessSongs(ArrayList<Song> hotSongs){
        for(Song s : hotSongs){
            maxHotnessAverages[0] += s.tempo;
            maxHotnessAverages[1] += s.timeSig;
            maxHotnessAverages[2] += s.duration;
            maxHotnessAverages[3] += s.mode;
            maxHotnessAverages[4] += s.loudness;
            maxHotnessAverages[5] += s.key;
            maxHotnessAverages[6] += s.fadeIn;
            maxHotnessAverages[7] += s.fadeOut;
        }

        for(int i = 0; i < maxHotnessAverages.length; i++){
            maxHotnessAverages[i] = maxHotnessAverages[i] / hotSongs.size();
        }
    }

    public void calculateCoefficients(){
        for(int i = 0; i < data.size(); i++){
            coefficients[i] = data.get(i).getCoefficient();
        }
    }

    public void calculateNewValues(){
        calculateCoefficients();

        mostPopTerms = Util.calculateMostPopularTerms(allTerms);

        //If coefficient is positive, increase by a factor of 0.3, if it's negative decrease by same factor
        for(int i = 0; i < coefficients.length; i++){
            if(coefficients[i] < 0){
                newValues[i] =  maxHotnessAverages[i] - (maxHotnessAverages[i] * 0.3);
            } else if(coefficients[i] > 0){
                newValues[i] =  maxHotnessAverages[i] + (maxHotnessAverages[i] * 0.3);
            } else {
                newValues[i] = maxHotnessAverages[i];
            }
        }
    }

    public double getNewTempo(){
        return newValues[0];
    }

    public int getNewTimeSig(){
        return (int) Math.ceil(newValues[1]);
    }

    public double getNewDuration(){
        return newValues[2];
    }

    public int getNewMode(){
        return (int) Math.ceil(newValues[3]);
    }

    public double getNewLoudness(){
        return newValues[4];
    }

    public int getNewKey(){
        return (int) Math.ceil(newValues[5]);
    }

    public double getNewFadeIn(){
        return newValues[6];
    }

    public double getNewFadeOut(){
        return newValues[7];
    }

    public String getNewTerms(){
        String s = "";

        for(String term : mostPopTerms){
            s += term + " ";
        }

        return s;
    }
}
