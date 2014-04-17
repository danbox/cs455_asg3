package readability.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import readability.util.FleschWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class FleschKincaidGradeLevelReducer extends Reducer<Text, FleschWritable, Text, DoubleWritable>
{
    public void reduce(Text key, Iterable<FleschWritable> values, Context context) throws IOException, InterruptedException
    {
        int totalSentences, totalWords, totalSyllables;
        totalSentences = totalWords = totalSyllables = 0;
        for(FleschWritable curr : values)
        {
            totalSentences += curr.getSentenceCount().get();
            totalWords += curr.getWordCount().get();
            totalSyllables += curr.getSyllableCount().get();
        }

        //calculate Flesch grade level
        double output = 0.39 * (totalWords / totalSentences) + 11.8 * (totalSyllables / totalWords) - 15.59;
        context.write(key, new DoubleWritable(output));
    }
}
