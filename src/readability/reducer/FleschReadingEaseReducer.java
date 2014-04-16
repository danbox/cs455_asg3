package readability.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import readability.util.FleschWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/15/14.
 */
public class FleschReadingEaseReducer extends Reducer<Text, FleschWritable, Text, DoubleWritable>
{
    public void reduce(Text key, Iterable<FleschWritable> values, Mapper.Context context) throws IOException, InterruptedException
    {
        int totalSentences, totalWords, totalSyllables;
        totalSentences = totalWords = totalSyllables = 0;
        for(FleschWritable curr : values)
        {
            totalSentences += curr.getSentenceCount().get();
            totalWords += curr.getWordCount().get();
            totalSyllables += curr.getSyllableCount().get();
        }

        //calculate Flesch reading ease
        double output = 206.835 - 1.015 * (totalWords / totalSentences) - 84.6 * (totalSyllables / totalWords);
        context.write(key, new DoubleWritable(output));
    }
}
