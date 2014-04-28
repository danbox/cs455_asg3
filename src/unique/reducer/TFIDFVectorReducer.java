package unique.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import unique.util.TermValueWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class TFIDFVectorReducer extends Reducer<IntWritable, TermValueWritable, IntWritable, Text>
{
    public void reduce(IntWritable key, Iterable<TermValueWritable> values, Context context) throws IOException, InterruptedException
    {
        StringBuilder stringBuilder = new StringBuilder();

        for(Iterator iterator = values.iterator(); iterator.hasNext();)
        {
            TermValueWritable curr = (TermValueWritable)iterator.next();
            stringBuilder.append("\n\t");
            stringBuilder.append(curr.toString());
        }

        context.write(key, new Text(stringBuilder.toString()));
    }
}
