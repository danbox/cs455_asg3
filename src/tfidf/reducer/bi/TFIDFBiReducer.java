package tfidf.reducer.bi;

import ngram.util.NGramWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author danbox
 * @date 4/29/14.
 */
public class TFIDFBiReducer extends Reducer<NGramWritableComparable, DoubleWritable, NGramWritableComparable, DoubleWritable>
{
    private Map<String, String> TFResults = new HashMap<String, String>();

    @Override
    public void reduce(NGramWritableComparable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
    {
        double sum = 0;
        for(DoubleWritable value : values)
        {
            sum += value.get();
        }

        if(TFResults.get(key.getTerm()) != null)
        {
            double tf = Double.parseDouble(TFResults.get(key.getTerm()));
            context.write(key, new DoubleWritable(tf * sum));
        }
    }

    @Override
    protected void setup(Context context) throws IOException
    {
        Path path = new Path("/books/temp/idf/part-r-00000");
        FileSystem fileSystem = FileSystem.get(new Configuration());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        String line;
        line = bufferedReader.readLine();
        while(line != null)
        {
            String[] split = line.split("\\s+");
            if(split.length == 3)
            {
                TFResults.put(split[0] + " " + split[1], split[2]);
            }
            line = bufferedReader.readLine();
        }
    }
}
