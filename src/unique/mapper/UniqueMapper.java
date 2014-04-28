package unique.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import unique.util.DecadeValueWritable;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class UniqueMapper extends Mapper<LongWritable, Text, Text, DecadeValueWritable>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get text
        String line = value.toString();

        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            String filename = tok.nextToken();
            StringBuilder stringBuilder = new StringBuilder();
            if(Character.isDigit(filename.charAt(11)))
            {
                stringBuilder.append(filename.substring(8, 12));
            } else
            {
                if(Character.isAlphabetic(filename.charAt(11)))
                {
                    stringBuilder.append('-');
                    stringBuilder.append(filename.substring(8, 11));
                } else
                {
                    stringBuilder.append(filename.substring(8, 11));
                }
            }


            int decade = (Integer.parseInt(stringBuilder.toString()) / 10) * 10;
            String ngram = tok.nextToken();
            double val = Double.parseDouble(tok.nextToken());

            context.write(new Text(ngram), new DecadeValueWritable(new IntWritable(decade), new DoubleWritable(val)));
        }
    }
}
