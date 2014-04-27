package readability.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import readability.mapper.FleschAverageMapper;
import readability.mapper.FleschMapper;
import readability.reducer.FleschAverageReducer;
import readability.reducer.FleschReadingEaseHistogramReducer;
import readability.util.DocumentComparator;
import readability.util.FleschWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class FleschReadingEaseAverage
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FleschReadingEaseAverage.class); // this class’s name
        job.setJobName("Flesch Reading Ease Histogram"); // name of this job.
        FileSystem fs = FileSystem.get(conf); // get the FS, you will need to initialize it
        FileStatus[] status_list = fs.listStatus(new Path(args[0]));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                // add each file to the list of inputs for the map-reduce job
                FileInputFormat.addInputPath(job, status.getPath());
            }
        }
        FileOutputFormat.setOutputPath(job, new Path("/books/temp/fre/")); // output path
        job.setMapperClass(FleschMapper.class); // mapper class
        job.setReducerClass(FleschReadingEaseHistogramReducer.class); // reducer class
        job.setSortComparatorClass(DocumentComparator.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FleschWritable.class);
        job.setOutputKeyClass(Text.class); // the key
        job.setOutputValueClass(DoubleWritable.class); //the value
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(FleschReadingEaseAverage.class); //this class's name
        job2.setJobName("Flesch Reading Ease Average"); //name of this job2.
        FileInputFormat.addInputPath(job2, new Path("/books/temp/fre/")); //input path
        FileOutputFormat.setOutputPath(job2, new Path(args[1])); //output path
        job2.setMapperClass(FleschAverageMapper.class); //test.mapper class
        job2.setCombinerClass(FleschAverageReducer.class); //optional
        job2.setReducerClass(FleschAverageReducer.class); //reducer class
        job2.setOutputKeyClass(IntWritable.class); // the key your reducer outputs
        job2.setOutputValueClass(DoubleWritable.class); // the value
//        job2.setGroupingComparatorClass(NGramGroupingComparator.class);
//        job2.setPartitionerClass(TermPartitioner.class);
//        job2.setSortComparatorClass(NGramComparator.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
