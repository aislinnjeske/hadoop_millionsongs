package cs455.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MillionSongJob {
    public static void main(String[] args){
        try {

            Configuration conf = new Configuration();

            //Give the MapReduce job a name. You'll see this name in the Yarn webapp
            Job job = Job.getInstance(conf, "Million Song Analysis");

            job.setJarByClass(MillionSongJob.class);
            job.setMapperClass(MillionSongMapperMetadata.class);
            job.setMapperClass(MillionSongMapperAnalysis.class);

//            job.setCombinerClass(MillionSongReducer.class);
            job.setReducerClass(MillionSongReducer.class);

            //Done by default?
            job.setNumReduceTasks(1);

            //Outputs from the mapper
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);

            //Outputs from the reducer
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //Path to input HDFS
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MillionSongMapperAnalysis.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MillionSongMapperMetadata.class);
            //Path to output HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            //Block until the job is completed
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(Exception e){
            System.err.println(e.getMessage());
        }

    }
}

