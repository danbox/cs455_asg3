package tfidf.runner;

import ngram.mapper.BigramCalculationMapper;
import ngram.mapper.FourGramCalculationMapper;
import ngram.mapper.TrigramCalculationMapper;
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
import tfidf.mapper.bi.IDFBiMapper;
import tfidf.mapper.bi.MaxTermBiMapper;
import tfidf.mapper.bi.TFBiMapper;
import tfidf.mapper.bi.TFIDFBiMapper;
import tfidf.mapper.four.IDFFourMapper;
import tfidf.mapper.four.MaxTermFourMapper;
import tfidf.mapper.four.TFFourMapper;
import tfidf.mapper.four.TFIDFFourMapper;
import tfidf.mapper.tri.IDFTriMapper;
import tfidf.mapper.tri.MaxTermTriMapper;
import tfidf.mapper.tri.TFIDFTriMapper;
import tfidf.mapper.tri.TFTriMapper;
import tfidf.mapper.uni.IDFUniMapper;
import tfidf.mapper.uni.MaxTermUniMapper;
import tfidf.mapper.uni.TFIDFUniMapper;
import tfidf.mapper.uni.TFUniMapper;
import tfidf.reducer.IDFReducer;
import tfidf.reducer.MaxTermReducer;
import tfidf.reducer.bi.TFIDFBiReducer;
import tfidf.reducer.TFReducer;
import tfidf.reducer.four.TFIDFFourReducer;
import tfidf.reducer.tri.TFIDFTriReducer;
import tfidf.reducer.uni.TFIDFUniReducer;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class TermFrequency
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
//        boolean success = false;
//        for(int i = 1; i <= 1; ++ i)
//        {
//            success = runJob(args[0], args[1], i);
//        }
//        System.exit(success ? 0 : 1);

        //N-Gram uniJob
        Job uniJob = Job.getInstance(new Configuration());
        uniJob.setJarByClass(TermFrequency.class); //this class's name
        uniJob.setJobName("N-Gram Calculation"); //name of this uniJob.
        FileInputFormat.addInputPath(uniJob, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(uniJob, new Path("/books/temp/ng")); //output path
        uniJob.setMapperClass(UnigramCalculationMapper.class); //test.mapper class
        uniJob.setCombinerClass(NGramCalculationReducer.class); //optional
        uniJob.setReducerClass(NGramCalculationReducer.class); //reducer class
        uniJob.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        uniJob.setOutputValueClass(IntWritable.class); // the value
        uniJob.waitForCompletion(true);

        //max term uniJob
        Job uniJob2 = Job.getInstance(new Configuration());
        uniJob2.setJarByClass(TermFrequency.class); //this class's name
        uniJob2.setJobName("Max Term"); //name of this uniJob2.
        FileInputFormat.addInputPath(uniJob2, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(uniJob2, new Path("/books/temp/max/")); //output path
        uniJob2.setMapperClass(MaxTermUniMapper.class); //test.mapper class
        uniJob2.setCombinerClass(MaxTermReducer.class); //optional
        uniJob2.setReducerClass(MaxTermReducer.class); //reducer class
        uniJob2.setOutputKeyClass(Text.class); // the key your reducer outputs
        uniJob2.setOutputValueClass(IntWritable.class); // the value
        uniJob2.waitForCompletion(true);

        //TF uniJob
        Job uniJob3 = Job.getInstance(new Configuration());
        uniJob3.setJarByClass(TermFrequency.class); //this class's name
        uniJob3.setJobName("TF"); //name of this uniJob3.
        FileInputFormat.addInputPath(uniJob3, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(uniJob3, new Path("/books/temp/tf")); //output path
        uniJob3.setMapperClass(TFUniMapper.class); //test.mapper class
        uniJob3.setReducerClass(TFReducer.class); //reducer class
        uniJob3.setMapOutputValueClass(IntWritable.class);
        uniJob3.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        uniJob3.setOutputValueClass(DoubleWritable.class); // the value
        uniJob3.waitForCompletion(true);

        //IDF uniJob
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(args[0]);
        configuration.setLong("fileCount", fileSystem.listStatus(path).length);
        Job uniJob4 = Job.getInstance(configuration);
        uniJob4.setJarByClass(TermFrequency.class); //this class's name
        uniJob4.setJobName("IDF"); //name of this uniJob4.
        FileInputFormat.addInputPath(uniJob4, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(uniJob4, new Path("/books/temp/idf")); //output path
        uniJob4.setMapperClass(IDFUniMapper.class); //test.mapper class
        uniJob4.setReducerClass(IDFReducer.class); //reducer class
        uniJob4.setMapOutputValueClass(IntWritable.class);
        uniJob4.setOutputKeyClass(Text.class); // the key your reducer outputs
        uniJob4.setOutputValueClass(DoubleWritable.class); // the value
        uniJob4.waitForCompletion(true);

        //TFIDF uniJob
        Job uniJob5 = Job.getInstance(new Configuration());
        uniJob5.setJarByClass(TermFrequency.class); //this class's name
        uniJob5.setJobName("TFIDF"); //name of this uniJob5.
        FileInputFormat.addInputPath(uniJob5, new Path("/books/temp/tf/")); //input path
        String output = args[1] + "/uni";
        FileOutputFormat.setOutputPath(uniJob5, new Path(output)); //output path
        uniJob5.setMapperClass(TFIDFUniMapper.class); //test.mapper class
        uniJob5.setReducerClass(TFIDFUniReducer.class); //reducer class
        uniJob5.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        uniJob5.setOutputValueClass(DoubleWritable.class); // the value
        uniJob5.waitForCompletion(true);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("/books/temp/"), true);

        //N-Gram biJob
        Job biJob = Job.getInstance(new Configuration());
        biJob.setJarByClass(TermFrequency.class); //this class's name
        biJob.setJobName("N-Gram Calculation"); //name of this biJob.
        FileInputFormat.addInputPath(biJob, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(biJob, new Path("/books/temp/ng")); //output path
        biJob.setMapperClass(BigramCalculationMapper.class); //test.mapper class
        biJob.setCombinerClass(NGramCalculationReducer.class); //optional
        biJob.setReducerClass(NGramCalculationReducer.class); //reducer class
        biJob.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        biJob.setOutputValueClass(IntWritable.class); // the value
        biJob.waitForCompletion(true);

        //max term biJob
        Job biJob2 = Job.getInstance(new Configuration());
        biJob2.setJarByClass(TermFrequency.class); //this class's name
        biJob2.setJobName("Max Term"); //name of this biJob2.
        FileInputFormat.addInputPath(biJob2, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(biJob2, new Path("/books/temp/max/")); //output path
        biJob2.setMapperClass(MaxTermBiMapper.class); //test.mapper class
        biJob2.setCombinerClass(MaxTermReducer.class); //optional
        biJob2.setReducerClass(MaxTermReducer.class); //reducer class
        biJob2.setOutputKeyClass(Text.class); // the key your reducer outputs
        biJob2.setOutputValueClass(IntWritable.class); // the value
        biJob2.waitForCompletion(true);

        //TF biJob
        Job biJob3 = Job.getInstance(new Configuration());
        biJob3.setJarByClass(TermFrequency.class); //this class's name
        biJob3.setJobName("TF"); //name of this biJob3.
        FileInputFormat.addInputPath(biJob3, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(biJob3, new Path("/books/temp/tf")); //output path
        biJob3.setMapperClass(TFBiMapper.class); //test.mapper class
        biJob3.setReducerClass(TFReducer.class); //reducer class
        biJob3.setMapOutputValueClass(IntWritable.class);
        biJob3.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        biJob3.setOutputValueClass(DoubleWritable.class); // the value
        biJob3.waitForCompletion(true);

        //IDF biJob
        Configuration configuration2 = new Configuration();
        FileSystem fileSystem2 = FileSystem.get(configuration2);
        Path path2 = new Path(args[0]);
        configuration2.setLong("fileCount", fileSystem2.listStatus(path2).length);
        Job biJob4 = Job.getInstance(configuration2);
        biJob4.setJarByClass(TermFrequency.class); //this class's name
        biJob4.setJobName("IDF"); //name of this biJob4.
        FileInputFormat.addInputPath(biJob4, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(biJob4, new Path("/books/temp/idf")); //output path
        biJob4.setMapperClass(IDFBiMapper.class); //test.mapper class
        biJob4.setReducerClass(IDFReducer.class); //reducer class
        biJob4.setMapOutputValueClass(IntWritable.class);
        biJob4.setOutputKeyClass(Text.class); // the key your reducer outputs
        biJob4.setOutputValueClass(DoubleWritable.class); // the value
        biJob4.waitForCompletion(true);

        //TFIDF biJob
        Job biJob5 = Job.getInstance(new Configuration());
        biJob5.setJarByClass(TermFrequency.class); //this class's name
        biJob5.setJobName("TFIDF"); //name of this biJob5.
        FileInputFormat.addInputPath(biJob5, new Path("/books/temp/tf/")); //input path
        output = args[1] + "/bi";
        FileOutputFormat.setOutputPath(biJob5, new Path(output)); //output path
        biJob5.setMapperClass(TFIDFBiMapper.class); //test.mapper class
        biJob5.setReducerClass(TFIDFBiReducer.class); //reducer class
        biJob5.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        biJob5.setOutputValueClass(DoubleWritable.class); // the value
        biJob5.waitForCompletion(true);

        FileSystem fs2 = FileSystem.get(new Configuration());
        fs2.delete(new Path("/books/temp/"), true);
        
//        System.exit(success ? 0 : 1);

        //N-Gram triJob
        Job triJob = Job.getInstance(new Configuration());
        triJob.setJarByClass(TermFrequency.class); //this class's name
        triJob.setJobName("N-Gram Calculation"); //name of this triJob.
        FileInputFormat.addInputPath(triJob, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(triJob, new Path("/books/temp/ng")); //output path
        triJob.setMapperClass(TrigramCalculationMapper.class); //test.mapper class
        triJob.setCombinerClass(NGramCalculationReducer.class); //optional
        triJob.setReducerClass(NGramCalculationReducer.class); //reducer class
        triJob.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        triJob.setOutputValueClass(IntWritable.class); // the value
        triJob.waitForCompletion(true);

        //max term triJob
        Job triJob2 = Job.getInstance(new Configuration());
        triJob2.setJarByClass(TermFrequency.class); //this class's name
        triJob2.setJobName("Max Term"); //name of this triJob2.
        FileInputFormat.addInputPath(triJob2, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(triJob2, new Path("/books/temp/max/")); //output path
        triJob2.setMapperClass(MaxTermTriMapper.class); //test.mapper class
        triJob2.setCombinerClass(MaxTermReducer.class); //optional
        triJob2.setReducerClass(MaxTermReducer.class); //reducer class
        triJob2.setOutputKeyClass(Text.class); // the key your reducer outputs
        triJob2.setOutputValueClass(IntWritable.class); // the value
        triJob2.waitForCompletion(true);

        //TF triJob
        Job triJob3 = Job.getInstance(new Configuration());
        triJob3.setJarByClass(TermFrequency.class); //this class's name
        triJob3.setJobName("TF"); //name of this triJob3.
        FileInputFormat.addInputPath(triJob3, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(triJob3, new Path("/books/temp/tf")); //output path
        triJob3.setMapperClass(TFTriMapper.class); //test.mapper class
        triJob3.setReducerClass(TFReducer.class); //reducer class
        triJob3.setMapOutputValueClass(IntWritable.class);
        triJob3.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        triJob3.setOutputValueClass(DoubleWritable.class); // the value
        triJob3.waitForCompletion(true);

        //IDF triJob
        Configuration configuration3 = new Configuration();
        FileSystem fileSystem3 = FileSystem.get(configuration3);
        Path path3 = new Path(args[0]);
        configuration3.setLong("fileCount", fileSystem3.listStatus(path3).length);
        Job triJob4 = Job.getInstance(configuration3);
        triJob4.setJarByClass(TermFrequency.class); //this class's name
        triJob4.setJobName("IDF"); //name of this triJob4.
        FileInputFormat.addInputPath(triJob4, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(triJob4, new Path("/books/temp/idf")); //output path
        triJob4.setMapperClass(IDFTriMapper.class); //test.mapper class
        triJob4.setReducerClass(IDFReducer.class); //reducer class
        triJob4.setMapOutputValueClass(IntWritable.class);
        triJob4.setOutputKeyClass(Text.class); // the key your reducer outputs
        triJob4.setOutputValueClass(DoubleWritable.class); // the value
        triJob4.waitForCompletion(true);

        //TFIDF triJob
        Job triJob5 = Job.getInstance(new Configuration());
        triJob5.setJarByClass(TermFrequency.class); //this class's name
        triJob5.setJobName("TFIDF"); //name of this triJob5.
        FileInputFormat.addInputPath(triJob5, new Path("/books/temp/tf/")); //input path
        output = args[1] + "/tri";
        FileOutputFormat.setOutputPath(triJob5, new Path(output)); //output path
        triJob5.setMapperClass(TFIDFTriMapper.class); //test.mapper class
        triJob5.setReducerClass(TFIDFTriReducer.class); //reducer class
        triJob5.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        triJob5.setOutputValueClass(DoubleWritable.class); // the value
        triJob5.waitForCompletion(true);

        FileSystem fs3 = FileSystem.get(new Configuration());
        fs3.delete(new Path("/books/temp/"), true);

//        System.exit(success ? 0 : 1);

        //N-Gram fourJob
        Job fourJob = Job.getInstance(new Configuration());
        fourJob.setJarByClass(TermFrequency.class); //this class's name
        fourJob.setJobName("N-Gram Calculation"); //name of this fourJob.
        FileInputFormat.addInputPath(fourJob, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(fourJob, new Path("/books/temp/ng")); //output path
        fourJob.setMapperClass(FourGramCalculationMapper.class); //test.mapper class
        fourJob.setCombinerClass(NGramCalculationReducer.class); //optional
        fourJob.setReducerClass(NGramCalculationReducer.class); //reducer class
        fourJob.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        fourJob.setOutputValueClass(IntWritable.class); // the value
        fourJob.waitForCompletion(true);

        //max term fourJob
        Job fourJob2 = Job.getInstance(new Configuration());
        fourJob2.setJarByClass(TermFrequency.class); //this class's name
        fourJob2.setJobName("Max Term"); //name of this fourJob2.
        FileInputFormat.addInputPath(fourJob2, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(fourJob2, new Path("/books/temp/max/")); //output path
        fourJob2.setMapperClass(MaxTermFourMapper.class); //test.mapper class
        fourJob2.setCombinerClass(MaxTermReducer.class); //optional
        fourJob2.setReducerClass(MaxTermReducer.class); //reducer class
        fourJob2.setOutputKeyClass(Text.class); // the key your reducer outputs
        fourJob2.setOutputValueClass(IntWritable.class); // the value
        fourJob2.waitForCompletion(true);

        //TF fourJob
        Job fourJob3 = Job.getInstance(new Configuration());
        fourJob3.setJarByClass(TermFrequency.class); //this class's name
        fourJob3.setJobName("TF"); //name of this fourJob3.
        FileInputFormat.addInputPath(fourJob3, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(fourJob3, new Path("/books/temp/tf")); //output path
        fourJob3.setMapperClass(TFFourMapper.class); //test.mapper class
        fourJob3.setReducerClass(TFReducer.class); //reducer class
        fourJob3.setMapOutputValueClass(IntWritable.class);
        fourJob3.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        fourJob3.setOutputValueClass(DoubleWritable.class); // the value
        fourJob3.waitForCompletion(true);

        //IDF fourJob
        Configuration configuration4 = new Configuration();
        FileSystem fileSystem4 = FileSystem.get(configuration4);
        Path path4 = new Path(args[0]);
        configuration4.setLong("fileCount", fileSystem4.listStatus(path4).length);
        Job fourJob4 = Job.getInstance(configuration4);
        fourJob4.setJarByClass(TermFrequency.class); //this class's name
        fourJob4.setJobName("IDF"); //name of this fourJob4.
        FileInputFormat.addInputPath(fourJob4, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(fourJob4, new Path("/books/temp/idf")); //output path
        fourJob4.setMapperClass(IDFFourMapper.class); //test.mapper class
        fourJob4.setReducerClass(IDFReducer.class); //reducer class
        fourJob4.setMapOutputValueClass(IntWritable.class);
        fourJob4.setOutputKeyClass(Text.class); // the key your reducer outputs
        fourJob4.setOutputValueClass(DoubleWritable.class); // the value
        fourJob4.waitForCompletion(true);

        //TFIDF fourJob
        Job fourJob5 = Job.getInstance(new Configuration());
        fourJob5.setJarByClass(TermFrequency.class); //this class's name
        fourJob5.setJobName("TFIDF"); //name of this fourJob5.
        FileInputFormat.addInputPath(fourJob5, new Path("/books/temp/tf/")); //input path
        output = args[1] + "/four";
        FileOutputFormat.setOutputPath(fourJob5, new Path(output)); //output path
        fourJob5.setMapperClass(TFIDFFourMapper.class); //test.mapper class
        fourJob5.setReducerClass(TFIDFFourReducer.class); //reducer class
        fourJob5.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        fourJob5.setOutputValueClass(DoubleWritable.class); // the value
        boolean success = fourJob5.waitForCompletion(true);

        FileSystem fs4 = FileSystem.get(new Configuration());
        fs4.delete(new Path("/books/temp/"), true);

        System.exit(success ? 0 : 1);
    }

    private static boolean runJob(String in, String out, int gram) throws IOException, InterruptedException, ClassNotFoundException
    {
        //N-Gram job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TermFrequency.class); //this class's name
        job.setJobName("N-Gram Calculation"); //name of this job.
        FileInputFormat.addInputPath(job, new Path(in)); //input path
        FileOutputFormat.setOutputPath(job, new Path("/books/temp/ng")); //output path
        switch(gram)
        {
            case 1:
                job.setMapperClass(UnigramCalculationMapper.class); //mapper class
                break;
            case 2:
                job.setMapperClass(BigramCalculationMapper.class); //mapper class
                break;
            case 3:
                job.setMapperClass(TrigramCalculationMapper.class); //mapper class
                break;
            case 4:
                job.setMapperClass(FourGramCalculationMapper.class); //mapper class
                break;
            default:
                job.setMapperClass(UnigramCalculationMapper.class); //mapper class
        }
        job.setCombinerClass(NGramCalculationReducer.class); //optional
        job.setReducerClass(NGramCalculationReducer.class); //reducer class
        job.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job.setOutputValueClass(IntWritable.class); // the value
        job.waitForCompletion(true);

        //max term job
        Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(TermFrequency.class); //this class's name
        job2.setJobName("Max Term"); //name of this job2.
        FileInputFormat.addInputPath(job2, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job2, new Path("/books/temp/max/")); //output path
        switch(gram)
        {
            case 1:
                job2.setMapperClass(MaxTermUniMapper.class); //mapper class
                break;
            case 2:
                job2.setMapperClass(MaxTermBiMapper.class); //mapper class
                break;
            case 3:
                job2.setMapperClass(MaxTermUniMapper.class); //mapper class
                break;
            case 4:
                job2.setMapperClass(MaxTermUniMapper.class); //mapper class
                break;
            default:
                job2.setMapperClass(MaxTermUniMapper.class); //mapper class
        }
        job2.setCombinerClass(MaxTermReducer.class); //optional
        job2.setReducerClass(MaxTermReducer.class); //reducer class
        job2.setOutputKeyClass(Text.class); // the key your reducer outputs
        job2.setOutputValueClass(IntWritable.class); // the value
        job2.waitForCompletion(true);

        //TF job
        Job job3 = Job.getInstance(new Configuration());
        job3.setJarByClass(TermFrequency.class); //this class's name
        job3.setJobName("TF"); //name of this job3.
        FileInputFormat.addInputPath(job3, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job3, new Path("/books/temp/tf")); //output path
        switch(gram)
        {
            case 1:
                job2.setMapperClass(TFUniMapper.class); //mapper class
                job3.setReducerClass(TFReducer.class); //reducer class
                break;
            case 2:
                job2.setMapperClass(TFBiMapper.class); //mapper class
                job3.setReducerClass(TFReducer.class); //reducer class
                break;
            case 3:
                job2.setMapperClass(TFUniMapper.class); //mapper class
                job3.setReducerClass(TFReducer.class); //reducer class
                break;
            case 4:
                job2.setMapperClass(TFUniMapper.class); //mapper class
                job3.setReducerClass(TFReducer.class); //reducer class
                break;
            default:
                job2.setMapperClass(TFUniMapper.class); //mapper class
                job3.setReducerClass(TFReducer.class); //reducer class
        }
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job3.setOutputValueClass(DoubleWritable.class); // the value
        job3.waitForCompletion(true);

        //IDF job
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(in);
        configuration.setLong("fileCount", fileSystem.listStatus(path).length);
        Job job4 = Job.getInstance(configuration);
        job4.setJarByClass(TermFrequency.class); //this class's name
        job4.setJobName("IDF"); //name of this job4.
        FileInputFormat.addInputPath(job4, new Path("/books/temp/ng/")); //input path
        FileOutputFormat.setOutputPath(job4, new Path("/books/temp/idf")); //output path
        switch(gram)
        {
            case 1:
                job2.setMapperClass(IDFUniMapper.class); //mapper class
                break;
            case 2:
                job2.setMapperClass(IDFBiMapper.class); //mapper class
                break;
            case 3:
                job2.setMapperClass(IDFUniMapper.class); //mapper class
                break;
            case 4:
                job2.setMapperClass(IDFUniMapper.class); //mapper class
                break;
            default:
                job2.setMapperClass(IDFUniMapper.class); //mapper class
        }
        job4.setReducerClass(IDFReducer.class); //reducer class
        job4.setMapOutputValueClass(IntWritable.class);
        job4.setOutputKeyClass(Text.class); // the key your reducer outputs
        job4.setOutputValueClass(DoubleWritable.class); // the value
        job4.waitForCompletion(true);

        //TFIDF job
        Job job5 = Job.getInstance(new Configuration());
        job5.setJarByClass(TermFrequency.class); //this class's name
        job5.setJobName("TFIDF"); //name of this job5.
        FileInputFormat.addInputPath(job5, new Path("/books/temp/tf/")); //input path
        String output;
        switch(gram)
        {
            case 1:
                output = out + "/uni";
                job5.setMapperClass(TFIDFUniMapper.class); //mapper class
                job5.setReducerClass(TFIDFUniReducer.class); //reducer class
                break;
            case 2:
                output = out + "/bi";
                job5.setMapperClass(TFIDFBiMapper.class); //mapper class
                job5.setReducerClass(TFIDFBiReducer.class); //reducer class
                break;
            case 3:
                output = out + "/tri";
                job5.setMapperClass(TFIDFUniMapper.class); //mapper class
                job5.setReducerClass(TFIDFUniReducer.class); //reducer class
                break;
            case 4:
                output = out + "/four";
                job5.setMapperClass(TFIDFUniMapper.class); //mapper class
                job5.setReducerClass(TFIDFUniReducer.class); //reducer class
                break;
            default:
                output = out + "/uni";
                job5.setMapperClass(TFIDFUniMapper.class); //mapper class
                job5.setReducerClass(TFIDFUniReducer.class); //reducer class
                break;
        }
        FileOutputFormat.setOutputPath(job5, new Path(output)); //output path
        job5.setOutputKeyClass(NGramWritableComparable.class); // the key your reducer outputs
        job5.setOutputValueClass(DoubleWritable.class); // the value
        boolean success = job5.waitForCompletion(true);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("/books/temp/"), true);

        return success;
    }
}
