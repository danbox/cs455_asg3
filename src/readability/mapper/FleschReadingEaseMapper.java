package readability.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import readability.util.FleschWritable;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author danbox
 * @date 4/15/14.
 */

public class FleschReadingEaseMapper extends Mapper<LongWritable, Text, Text, FleschWritable>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //get filename
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();

        //get text
        String line = value.toString();

        StringTokenizer tok = new StringTokenizer(line);
        while(tok.hasMoreTokens())
        {
            String curr = tok.nextToken();
            char lastChar = curr.charAt(curr.length() - 1);

            int sentenceCount = 0;
            if(lastChar == '.' || lastChar == '!' || lastChar == '?')
            {
                sentenceCount = 1;
            }

            int wordCount = 1;
            int syllableCount = countSyllables(curr);

            FleschWritable fleschWritable = new FleschWritable(new IntWritable(sentenceCount), new IntWritable(wordCount), new IntWritable(syllableCount));
            context.write(new Text(filename), fleschWritable);
        }
    }

    private int countSyllables(String word)
    {
        char[] vowels = { 'a', 'e', 'i', 'o', 'u', 'y' };
        String currentWord = word;
        int numVowels = 0;
        Boolean lastWasVowel = false;
        for(char wc : currentWord.toCharArray())
        {
            Boolean foundVowel = false;
            for(char v : vowels)
            {
                //don't count diphthongs
                if (v == wc && lastWasVowel)
                {
                    foundVowel = true;
                    lastWasVowel = true;
                    break;
                }
                else if (v == wc && !lastWasVowel)
                {
                    numVowels++;
                    foundVowel = true;
                    lastWasVowel = true;
                    break;
                }
            }

            //if full cycle and no vowel found, set lastWasVowel to false;
            if (!foundVowel)
                lastWasVowel = false;
        }
        //remove es, it's _usually? silent
        if (currentWord.length() > 2 && currentWord.substring(currentWord.length() - 2) == "es")
            numVowels--;
            // remove silent e
        else if (currentWord.length() > 1 && currentWord.substring(currentWord.length() - 1) == "e")
            numVowels--;

        return numVowels;
    }
}
