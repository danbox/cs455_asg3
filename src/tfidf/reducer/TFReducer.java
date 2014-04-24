package tfidf.reducer;

import ngram.util.NGramWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author danbox
 * @date 4/23/14.
 */
public class TFReducer extends Reducer<NGramWritableComparable, IntWritable, NGramWritableComparable, DoubleWritable>
{
    private Map<String, String> maxTerms = new HashMap<String, String>();

    public void reduce(NGramWritableComparable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for(IntWritable value : values)
        {
            sum += value.get();
        }
        if(maxTerms.get(key.getFilename()) != null)
        {
            int max = Integer.parseInt(maxTerms.get(key.getFilename()));
            context.write(key, new DoubleWritable(.5 + ((.5 * sum) / max)));
        }
    }

    @Override
    protected void setup(Context context) throws IOException
    {
        Path path = new Path("/books/temp/max/part-r-00000");
        FileSystem fileSystem = FileSystem.get(new Configuration());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        String line;
        line = bufferedReader.readLine();
        while(line != null)
        {
            String[] split = line.split("\\s+");
            maxTerms.put(split[0], split[1]);
            line = bufferedReader.readLine();
        }
    }
}
