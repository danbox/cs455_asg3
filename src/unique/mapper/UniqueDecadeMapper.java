package unique.mapper;

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
public class UniqueDecadeMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get text
        String line = value.toString();

        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            context.write(new IntWritable(Integer.parseInt(tok.nextToken())), new Text(tok.nextToken()));
            tok.nextToken();
        }
    }
}
