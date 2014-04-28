package unique.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import unique.util.TermValueWritable;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class TFIDFVectorMapper extends Mapper<LongWritable, Text, IntWritable, TermValueWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get text
        String line = value.toString();

        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            context.write(new IntWritable(Integer.parseInt(tok.nextToken())), new TermValueWritable(new Text(tok.nextToken()), new DoubleWritable(Double.parseDouble(tok.nextToken()))));
        }
    }
}
