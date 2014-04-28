package unique.reducer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import unique.util.DecadeValueWritable;
import unique.util.TermValueWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class UniqueReducer extends Reducer<Text, DecadeValueWritable, IntWritable, TermValueWritable>
{
    public void reduce(Text key, Iterable<DecadeValueWritable> values, Context context) throws IOException, InterruptedException
    {
        double val = 0;
        int decade = 0;
        int numElements = 0;
        for(DecadeValueWritable curr : values)
        {
            decade = curr.getDecade().get();
            val = curr.getVal().get();
            numElements += 1;
        }

        if(numElements == 1)
        {
            context.write(new IntWritable(decade), new TermValueWritable(key, new DoubleWritable(val)));
        }
    }
}
