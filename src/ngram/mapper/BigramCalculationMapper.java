package ngram.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/16/14.
 */

public class BigramCalculationMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        StringTokenizer tok = new StringTokenizer(line);
        String first = null;
        if(tok.hasMoreTokens())
        {
            first = tok.nextToken();
        }
        while(tok.hasMoreTokens())
        {
                String second = tok.nextToken();
                context.write(new Text(first + " " +  second), new IntWritable(1));
                first = second;
        }
    }
}
