package test.runner;

import test.mapper.YarnWordCountMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import test.reducer.YarnWordCountReducer;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/14/14.
 */
public class WordCountRunner
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCountRunner.class); //this class's name
        job.setJobName("Word Count"); //name of this job.
        FileInputFormat.addInputPath(job, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //output path
        job.setMapperClass(YarnWordCountMapper.class); //test.mapper class
        job.setCombinerClass(YarnWordCountReducer.class); //optional
        job.setReducerClass(YarnWordCountReducer.class); //test.reducer class
        job.setOutputKeyClass(Text.class); // the key your test.reducer outputs
        job.setOutputValueClass(IntWritable.class); // the value
        System.exit( job.waitForCompletion(true) ? 0 : 1);

    }
}
