package tfidf.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import tfidf.util.TermFrequencyWritable;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class TermFrequencyMapper extends Mapper<LongWritable, Text, Text, TermFrequencyWritable>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get filename
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();

        //get text
        String line = value.toString();

        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            TermFrequencyWritable termFrequencyWritable = new TermFrequencyWritable(tok.nextToken(), 1);
            context.write(new Text(filename), termFrequencyWritable);
        }
    }
}
