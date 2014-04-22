package ngram.reducer;

import ngram.util.NGramWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class NGramCalculationReducer extends Reducer<NGramWritable, IntWritable, NGramWritable, IntWritable>
{
    public void reduce(NGramWritable key, Iterable<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for(IntWritable value : values)
        {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
