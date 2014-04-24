package tfidf.reducer;

import ngram.util.NGramWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import tfidf.util.FileCounter;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/23/14.
 */
public class IDFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>
{

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for(IntWritable value : values)
        {
            sum += value.get();
        }
        context.write(key, new DoubleWritable(Math.log(context.getConfiguration().getLong("fileCount", 0) / sum)));
    }

}