package ngram.mapper;

import ngram.util.NGramWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/16/14.
 */

public class UnigramCalculationMapper extends Mapper<LongWritable, Text, NGramWritable, IntWritable>
{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException
    {
        //get filename
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();

        String line = value.toString();
        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            context.write(new NGramWritable(filename, tok.nextToken()), new IntWritable(1));
        }
    }
}
