package unique.runner;

import ngram.mapper.UnigramCalculationMapper;
import ngram.reducer.NGramCalculationReducer;
import ngram.util.NGramWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import unique.mapper.UniqueDecadeMapper;
import unique.mapper.UniqueMapper;
import unique.reducer.UniqueDecadeReducer;
import unique.reducer.UniqueReducer;
import unique.util.DecadeValueWritable;
import unique.util.TermValueWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class Unique
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        //N-Gram job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Unique.class); //this class's name
        job.setJobName("N-Gram Calculation"); //name of this job.
        FileInputFormat.addInputPath(job, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(job, new Path("/books/temp/ng")); //output path
        job.setMapperClass(UnigramCalculationMapper.class); //test.mapper class
        job.setCombinerClass(NGramCalculationReducer.class); //optional
        job.setReducerClass(NGramCalculationReducer.class); //reducer class
        job.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job.setOutputValueClass(IntWritable.class); // the value
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(Unique.class); //this class's name
        job2.setJobName("Unique"); //name of this job2.
        FileInputFormat.addInputPath(job2, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job2, new Path("/books/temp/uniq/")); //output path
        job2.setMapperClass(UniqueMapper.class); //test.mapper class
//        job2.setCombinerClass(NGramCalculationReducer.class); //optional
        job2.setReducerClass(UniqueReducer.class); //reducer class
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DecadeValueWritable.class);
        job2.setOutputKeyClass(IntWritable.class); // the key your reducer outputs
        job2.setOutputValueClass(TermValueWritable.class); // the value
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(new Configuration());
        job3.setJarByClass(Unique.class); //this class's name
        job3.setJobName("Unique Per Decade"); //name of this job3.
        FileInputFormat.addInputPath(job3, new Path("/books/temp/uniq/")); //input path
        FileOutputFormat.setOutputPath(job3, new Path(args[1])); //output path
        job3.setMapperClass(UniqueDecadeMapper.class); //test.mapper class
        job3.setReducerClass(UniqueDecadeReducer.class); //reducer class
        job3.setOutputKeyClass(IntWritable.class); // the key your reducer outputs
        job3.setOutputValueClass(Text.class); // the value
        System.exit( job3.waitForCompletion(true) ? 0 : 1);
    }
}
