package tfidf.mapper;

import ngram.util.NGramWritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/24/14.
 */
public class TFIDFMapper extends Mapper<LongWritable, Text, NGramWritableComparable, DoubleWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get text
        String line = value.toString();

        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            context.write(new NGramWritableComparable(tok.nextToken(), tok.nextToken()), new DoubleWritable(Double.parseDouble(tok.nextToken())));
        }
    }
}