package cs455.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;

public class MillionSongMapperMetadata extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Mapper value output: title%artist_id%artist_name%latitude%longitude%terms

        //Terms are space separated (should be a string array)

        ArrayList<String> data = splitString(value.toString());

        String outputKey = data.get(8);
        String outputValue = "%t" + data.get(9) + "%a" + data.get(3) + "%a" + data.get(7) + "%l" + data.get(4) + "%l" + data.get(5) + "%t" + data.get(11);

        context.write(new Text(outputKey), new Text(outputValue));
    }

    public ArrayList<String> splitString(String s){
        ArrayList<String> words = new ArrayList<>();
        boolean notInsideQuotes = true;
        int start = 0;
        int end = 0;

        for(int i = 0; i < s.length() - 1; i++){
            if(s.charAt(i) == ',' && notInsideQuotes){
                words.add(s.substring(start, i));
                start = i + 1;
            } else if(s.charAt(i) == '"'){
                notInsideQuotes = !notInsideQuotes;
            }
        }
        words.add(s.substring(start));
        return words;
    }

}
