package readability.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class FleschAverageMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get text
        String line = value.toString();

        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            int decade = (Integer.parseInt(tok.nextToken()) / 10) * 10;
            double val = Double.parseDouble(tok.nextToken());

            context.write(new IntWritable(decade), new DoubleWritable(val));
        }
    }
}
