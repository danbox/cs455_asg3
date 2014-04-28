package unique.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class UniqueDecadeReducer extends Reducer<IntWritable, Text, IntWritable, Text>
{
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        StringBuilder stringBuilder = new StringBuilder();

        for(Iterator iterator = values.iterator(); iterator.hasNext();)
        {
            Text curr = (Text)iterator.next();
            stringBuilder.append(curr.toString());
            if(iterator.hasNext())
            {
                stringBuilder.append(", ");
            }
        }

        context.write(key, new Text(stringBuilder.toString()));
    }
}
