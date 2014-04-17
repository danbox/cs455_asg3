package ngram.runner;

import ngram.mapper.TrigramCalculationMapper;
import ngram.mapper.FourGramCalculationMapper;
import ngram.reducer.NGramCalculationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class NGramCalculation
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(NGramCalculation.class); //this class's name
        job.setJobName("N-gram Calculation"); //name of this job.
        FileInputFormat.addInputPath(job, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //output path
        job.setMapperClass(FourGramCalculationMapper.class); //test.mapper class
        job.setCombinerClass(NGramCalculationReducer.class); //optional
        job.setReducerClass(NGramCalculationReducer.class); //test.reducer class
        job.setOutputKeyClass(Text.class); // the key your test.reducer outputs
        job.setOutputValueClass(IntWritable.class); // the value
        System.exit( job.waitForCompletion(true) ? 0 : 1);
    }
}
