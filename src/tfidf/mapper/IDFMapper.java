package tfidf.mapper;

import ngram.util.NGramWritableComparable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import tfidf.util.FileCounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/22/14.
 */
public class IDFMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    List<String> documents = new ArrayList<String>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            tok.nextToken();
            if(tok.hasMoreTokens())
            {
                context.write(new Text(tok.nextToken()), new IntWritable(1));
            }
        }
    }
}
