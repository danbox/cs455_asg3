package tfidf.mapper.tri;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/29/14.
 */
public class IDFTriMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
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
                context.write(new Text(tok.nextToken() + " " + tok.nextToken() + " " + tok.nextToken()), new IntWritable(1));
            }
        }
    }
}
