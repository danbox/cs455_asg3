package readability.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import readability.mapper.FleschMapper;
import readability.reducer.FleschKincaidGradeLevelReducer;
import readability.util.FleschWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class FleschKincaidGradeLevel
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FleschKincaidGradeLevel.class); //this class’s name
        job.setJobName("Flesch Grade Level"); //name of this job.
        FileSystem fs = FileSystem.get(conf); //get the FS, you will need to initialize it
        FileStatus[] status_list = fs.listStatus(new Path(args[0]));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                //add each file to the list of inputs for the map-reduce job
                FileInputFormat.addInputPath(job, status.getPath());
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //output path
        job.setMapperClass(FleschMapper.class); //mapper class
        job.setReducerClass(FleschKincaidGradeLevelReducer.class); //reducer class
//        job.setSortComparatorClass(DocumentComparator.class); //comparator class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FleschWritable.class);
        job.setOutputKeyClass(Text.class); //the key
        job.setOutputValueClass(DoubleWritable.class); //the value
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
