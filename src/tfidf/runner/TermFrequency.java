package tfidf.runner;

import ngram.mapper.UnigramCalculationMapper;
import ngram.reducer.NGramCalculationReducer;
import ngram.util.NGramWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import tfidf.mapper.IDFMapper;
import tfidf.mapper.MaxTermMapper;
import tfidf.mapper.TFIDFMapper;
import tfidf.mapper.TFMapper;
import tfidf.reducer.IDFReducer;
import tfidf.reducer.MaxTermReducer;
import tfidf.reducer.TFIDFReducer;
import tfidf.reducer.TFReducer;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class TermFrequency
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        //N-Gram job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TermFrequency.class); //this class's name
        job.setJobName("N-Gram Calculation"); //name of this job.
        FileInputFormat.addInputPath(job, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(job, new Path("/books/temp/ng")); //output path
        job.setMapperClass(UnigramCalculationMapper.class); //test.mapper class
        job.setCombinerClass(NGramCalculationReducer.class); //optional
        job.setReducerClass(NGramCalculationReducer.class); //reducer class
        job.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job.setOutputValueClass(IntWritable.class); // the value
//        job.setGroupingComparatorClass(NGramGroupingComparator.class);
//        job.setPartitionerClass(TermPartitioner.class);
//        job.setSortComparatorClass(NGramComparator.class);
//        System.exit( job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);
        
        //max term job
        Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(TermFrequency.class); //this class's name
        job2.setJobName("Max Term"); //name of this job2.
        FileInputFormat.addInputPath(job2, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job2, new Path("/books/temp/max/")); //output path
        job2.setMapperClass(MaxTermMapper.class); //test.mapper class
        job2.setCombinerClass(MaxTermReducer.class); //optional
        job2.setReducerClass(MaxTermReducer.class); //reducer class
        job2.setOutputKeyClass(Text.class); // the key your reducer outputs
        job2.setOutputValueClass(IntWritable.class); // the value
//        job2.setGroupingComparatorClass(NGramGroupingComparator.class);
//        job2.setPartitionerClass(TermPartitioner.class);
//        job2.setSortComparatorClass(NGramComparator.class);
        job2.waitForCompletion(true);
        
        //TF job
        Job job3 = Job.getInstance(new Configuration());
        job3.setJarByClass(TermFrequency.class); //this class's name
        job3.setJobName("TF"); //name of this job3.
        FileInputFormat.addInputPath(job3, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job3, new Path("/books/temp/tf")); //output path
        job3.setMapperClass(TFMapper.class); //test.mapper class
//        job3.setCombinerClass(TFReducer.class); //optional
        job3.setReducerClass(TFReducer.class); //reducer class
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job3.setOutputValueClass(DoubleWritable.class); // the value
//        job3.setGroupingComparatorClass(NGramGroupingComparator.class);
//        job3.setPartitionerClass(TermPartitioner.class);
//        job3.setSortComparatorClass(NGramComparator.class);
//        System.exit( job3.waitForCompletion(true) ? 0 : 1);
        job3.waitForCompletion(true);


        //IDF job
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(args[0]);
        Configuration configuration = new Configuration();
        configuration.setLong("fileCount", fileSystem.listStatus(path).length);
        Job job4 = Job.getInstance(configuration);
        job4.setJarByClass(TermFrequency.class); //this class's name
        job4.setJobName("IDF"); //name of this job4.
        FileInputFormat.addInputPath(job4, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job4, new Path("/books/temp/idf")); //output path
        job4.setMapperClass(IDFMapper.class); //test.mapper class
//        job4.setCombinerClass(IDFReducer.class); //optional
        job4.setReducerClass(IDFReducer.class); //reducer class
        job4.setMapOutputValueClass(IntWritable.class);
        job4.setOutputKeyClass(Text.class); // the key your reducer outputs
        job4.setOutputValueClass(DoubleWritable.class); // the value
//        job4.setGroupingComparatorClass(NGramGroupingComparator.class);
//        job4.setPartitionerClass(TermPartitioner.class);
//        job4.setSortComparatorClass(NGramComparator.class);
//        System.exit( job4.waitForCompletion(true) ? 0 : 1);
        job4.waitForCompletion(true);

        //TFIDF job
        Job job5 = Job.getInstance(new Configuration());
        job5.setJarByClass(TermFrequency.class); //this class's name
        job5.setJobName("TFIDF"); //name of this job5.
        FileInputFormat.addInputPath(job5, new Path("/books/temp/tf/")); //input path
        FileOutputFormat.setOutputPath(job5, new Path(args[1])); //output path
        job5.setMapperClass(TFIDFMapper.class); //test.mapper class
//        job5.setCombinerClass(TFReducer.class); //optional
        job5.setReducerClass(TFIDFReducer.class); //reducer class
//        job5.setMapOutputValueClass(IntWritable.class);
        job5.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job5.setOutputValueClass(DoubleWritable.class); // the value
//        job5.setGroupingComparatorClass(NGramGroupingComparator.class);
//        job5.setPartitionerClass(TermPartitioner.class);
//        job5.setSortComparatorClass(NGramComparator.class);
        System.exit( job5.waitForCompletion(true) ? 0 : 1);
//        job5.waitForCompletion(true);
    }

}
