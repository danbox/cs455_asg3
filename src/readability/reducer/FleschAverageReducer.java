package readability.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import readability.util.FleschWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class FleschAverageReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>
{
    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
    {
        double sum = 0;
        int numElements = 0;
        for(DoubleWritable curr : values)
        {
            sum += curr.get();
            numElements += 1;
        }

        context.write(key, new DoubleWritable(sum / numElements));
    }
}
