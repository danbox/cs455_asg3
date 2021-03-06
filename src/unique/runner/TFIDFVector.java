package unique.runner;

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
import tfidf.mapper.uni.IDFUniMapper;
import tfidf.mapper.uni.MaxTermUniMapper;
import tfidf.mapper.uni.TFIDFUniMapper;
import tfidf.mapper.uni.TFUniMapper;
import tfidf.reducer.IDFReducer;
import tfidf.reducer.MaxTermReducer;
import tfidf.reducer.TFReducer;
import tfidf.reducer.uni.TFIDFUniReducer;
import unique.mapper.TFIDFVectorMapper;
import unique.mapper.UniqueMapper;
import unique.reducer.TFIDFVectorReducer;
import unique.reducer.UniqueReducer;
import unique.util.DecadeValueWritable;
import unique.util.TermValueWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class TFIDFVector
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {

        //N-Gram job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TFIDFVector.class); //this class's name
        job.setJobName("N-Gram Calculation"); //name of this job.
        FileInputFormat.addInputPath(job, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(job, new Path("/books/temp/ng")); //output path
        job.setMapperClass(UnigramCalculationMapper.class); //test.mapper class
        job.setCombinerClass(NGramCalculationReducer.class); //optional
        job.setReducerClass(NGramCalculationReducer.class); //reducer class
        job.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job.setOutputValueClass(IntWritable.class); // the value
        job.waitForCompletion(true);

        //max term job
        Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(TFIDFVector.class); //this class's name
        job2.setJobName("Max Term"); //name of this job2.
        FileInputFormat.addInputPath(job2, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job2, new Path("/books/temp/max/")); //output path
        job2.setMapperClass(MaxTermUniMapper.class); //test.mapper class
        job2.setCombinerClass(MaxTermReducer.class); //optional
        job2.setReducerClass(MaxTermReducer.class); //reducer class
        job2.setOutputKeyClass(Text.class); // the key your reducer outputs
        job2.setOutputValueClass(IntWritable.class); // the value
        job2.waitForCompletion(true);

        //TF job
        Job job3 = Job.getInstance(new Configuration());
        job3.setJarByClass(TFIDFVector.class); //this class's name
        job3.setJobName("TF"); //name of this job3.
        FileInputFormat.addInputPath(job3, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job3, new Path("/books/temp/tf")); //output path
        job3.setMapperClass(TFUniMapper.class); //test.mapper class
        job3.setReducerClass(TFReducer.class); //reducer class
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job3.setOutputValueClass(DoubleWritable.class); // the value
        job3.waitForCompletion(true);


        //IDF job
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(args[0]);
        Configuration configuration = new Configuration();
        configuration.setLong("fileCount", fileSystem.listStatus(path).length);
        Job job4 = Job.getInstance(configuration);
        job4.setJarByClass(TFIDFVector.class); //this class's name
        job4.setJobName("IDF"); //name of this job4.
        FileInputFormat.addInputPath(job4, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job4, new Path("/books/temp/idf")); //output path
        job4.setMapperClass(IDFUniMapper.class); //test.mapper class
        job4.setReducerClass(IDFReducer.class); //reducer class
        job4.setMapOutputValueClass(IntWritable.class);
        job4.setOutputKeyClass(Text.class); // the key your reducer outputs
        job4.setOutputValueClass(DoubleWritable.class); // the value
        job4.waitForCompletion(true);

        //TFIDF job
        Job job5 = Job.getInstance(new Configuration());
        job5.setJarByClass(TFIDFVector.class); //this class's name
        job5.setJobName("TFIDF"); //name of this job5.
        FileInputFormat.addInputPath(job5, new Path("/books/temp/tf/")); //input path
        FileOutputFormat.setOutputPath(job5, new Path("/books/temp/tfidf")); //output path
        job5.setMapperClass(TFIDFUniMapper.class); //test.mapper class
        job5.setReducerClass(TFIDFUniReducer.class); //reducer class
        job5.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job5.setOutputValueClass(DoubleWritable.class); // the value
        job5.waitForCompletion(true);

        Job job6 = Job.getInstance(new Configuration());
        job6.setJarByClass(Unique.class); //this class's name
        job6.setJobName("Unique"); //name of this job6.
        FileInputFormat.addInputPath(job6, new Path("/books/temp/tfidf/")); //input path
        FileOutputFormat.setOutputPath(job6, new Path("/books/temp/uniq/")); //output path
        job6.setMapperClass(UniqueMapper.class); //test.mapper class
        job6.setReducerClass(UniqueReducer.class); //reducer class
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(DecadeValueWritable.class);
        job6.setOutputKeyClass(IntWritable.class); // the key your reducer outputs
        job6.setOutputValueClass(TermValueWritable.class); // the value
        job6.waitForCompletion(true);

        Job job7 = Job.getInstance(new Configuration());
        job7.setJarByClass(Unique.class); //this class's name
        job7.setJobName("TFIDF Vector"); //name of this job7.
        FileInputFormat.addInputPath(job7, new Path("/books/temp/uniq/")); //input path
        FileOutputFormat.setOutputPath(job7, new Path(args[1])); //output path
        job7.setMapperClass(TFIDFVectorMapper.class); //test.mapper class
        job7.setReducerClass(TFIDFVectorReducer.class); //reducer class
        job7.setMapOutputValueClass(TermValueWritable.class);
        job7.setOutputKeyClass(IntWritable.class); // the key your reducer outputs
        job7.setOutputValueClass(Text.class); // the value
        System.exit( job7.waitForCompletion(true) ? 0 : 1);
    }
}
