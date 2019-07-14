package cs455.hadoop;

import cs455.hadoop.Utilities.Util;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Scanner;

public class MillionSongMapperAnalysis extends Mapper<LongWritable, Text, Text, Text>{

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //This assumes that the mapper gets one line of the csv at a time

        //Mapper value output: $hotness$duration$end_of_fade_in$key$loudness$mode$start_of_fade_out$tempo$time_sig$segments_pitches
        //$max_loudness$max_loud_time$start_loudness

        String[] data = value.toString().split(",");

        String outputKey = data[1];             //song_id

        if(isInValid(data[1])){
            return;
        }

        String outputValue = "@h" + data[2] + "@d" + data[5] + "@e" + data[6] + "@k" + data[8] + "@l" + data[10] + "@m" + data[11] +
                "@s" + data[13] + "@t" + data[14] + "@t" + data[15] + "@s" + segmentBreak(data[18]) + "@s" + segmentBreakPitch(data[20]) + "@t" + segmentBreakPitch(data[21]) + "@m" + segmentBreak(data[22]) +
                "@m" + segmentBreak(data[23]) + "@s" + segmentBreak(data[24]);

        context.write(new Text(outputKey), new Text(outputValue));
    }

    //Guarantees that segmentStart, maxLoudness, maxLoudnessTime, and maxLoudnessStart will all have 10 sections
    public String segmentBreak(String s){

        //Determine how many values will be in each section
        String[] sArr = s.split(" ");
        int numPerSection = (sArr.length / 10) + 1;

        //Creating segmented array and index for array
        double[] out = new double[10];
        int outIndex = 0;
        int sArrIndex = 0;

        //Creating the segments
        //Iterating through all the sections in out
        for(int i = 1; i < sArr.length; i += numPerSection){
            //Filling out a single section in out
            for(int j = 0; j < numPerSection && sArrIndex < sArr.length; j++){
                out[outIndex] += Util.parseDouble(sArr[sArrIndex]);
                sArrIndex++;
            }
            out[outIndex] = out[outIndex] / numPerSection;
            outIndex++;
        }

        return flattenArray(out);
    }

    //Guarantees that pitch and timbre have 12 sections
    public String segmentBreakPitch(String s){
        //Checking how many rows there are
        String[] sArr = s.split(" ");
        int rows = sArr.length / 12;

        //Output array and index for array
        double[] out = new double[12];
        int outIndex = 0;
        int sArrIndex = 0;

        //Iterates through out array
        for(int i = 0; i < 12; i++){
            //Gets 'row' number of values
            for(int j = 0; j < rows; j++){
                out[outIndex] += Util.parseDouble(sArr[sArrIndex]);
                sArrIndex++;
            }
            out[outIndex] = out[outIndex] / rows;
            outIndex++;
        }

        return flattenArray(out);
    }

    public String flattenArray(double[] array){
        String flatArray = "";
        for(int i = 0; i < array.length; i++){
            flatArray += array[i] + " ";
        }
        return flatArray;
    }

    public boolean isInValid(String s){
        return s.equals("song_id");
    }
}