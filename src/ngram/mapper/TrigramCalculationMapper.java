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
public class TrigramCalculationMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        StringTokenizer tok = new StringTokenizer(line);

        String first, second, third;
        first = second = third = null;
        if(tok.hasMoreTokens())
        {
            first = tok.nextToken();
            if(tok.hasMoreTokens())
            {
                second = tok.nextToken();
            }
        }

        while(tok.hasMoreTokens())
        {
            third = tok.nextToken();
            context.write(new Text(first + " " +  second + " " + third), new IntWritable(1));
            if(tok.hasMoreTokens())
            {
                first = tok.nextToken();
                if(tok.hasMoreTokens())
                {
                    second = tok.nextToken();
                }
            }
        }
    }
}
