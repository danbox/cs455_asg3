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
public class MaxTermTriMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get text
        String line = value.toString();

        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            String doc = tok.nextToken();
            tok.nextToken();
            tok.nextToken();
            tok.nextToken();

            context.write(new Text(doc), new IntWritable(Integer.parseInt(tok.nextToken())));
        }
    }
}

