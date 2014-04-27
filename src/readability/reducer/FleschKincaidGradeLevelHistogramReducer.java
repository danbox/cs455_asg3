package readability.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import readability.util.FleschWritable;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class FleschKincaidGradeLevelHistogramReducer extends Reducer<Text, FleschWritable, Text, DoubleWritable>
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

        StringBuilder stringBuilder = new StringBuilder();
        if(Character.isDigit(key.toString().charAt(11)))
        {
            stringBuilder.append(key.toString().substring(8, 12));
        } else
        {
            if(Character.isAlphabetic(key.toString().charAt(11)))
            {
                stringBuilder.append('-');
                stringBuilder.append(key.toString().substring(8, 11));
            } else
            {
                stringBuilder.append(key.toString().substring(8, 11));
            }
        }
        context.write(new Text(stringBuilder.toString()), new DoubleWritable(output));
    }
}
