package tfidf.reducer;

import ngram.util.NGramWritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import readability.util.FleschWritable;
import tfidf.util.TermFrequencyWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class MaxTermReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int max = 0;
        for(IntWritable value : values)
        {
            if(max < value.get())
            {
                max = value.get();
            }
        }
        context.write(key, new IntWritable(max));
    }

}
