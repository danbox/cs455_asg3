package ngram.mapper;

import ngram.reducer.NGramCalculationReducer;
import ngram.util.NGramWritableComparable;
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
public class TrigramCalculationMapper extends Mapper<LongWritable, Text, NGramWritableComparable, IntWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get filename
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();

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
            context.write(new NGramWritableComparable(filename, first + " " +  second + " " + third), new IntWritable(1));
            first = second;
            second = third;
        }
    }
}
