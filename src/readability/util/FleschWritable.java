package readability.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author danbox
 * @date 4/15/14.
 */
public class FleschWritable implements Writable
{
    private IntWritable _sentenceCount;
    private IntWritable _wordCount;
    private IntWritable _syllableCount;

    public FleschWritable()
    {
        _sentenceCount = new IntWritable();
        _wordCount = new IntWritable();
        _syllableCount = new IntWritable();
    }

    public FleschWritable(IntWritable sentenceCount, IntWritable wordCount, IntWritable syllableCount)
    {
        _sentenceCount = sentenceCount;
        _wordCount = wordCount;
        _syllableCount = syllableCount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        _sentenceCount.write(dataOutput);
        _wordCount.write(dataOutput);
        _syllableCount.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        _sentenceCount.readFields(dataInput);
        _wordCount.readFields(dataInput);
        _syllableCount.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return "Sentence count: " + _sentenceCount + " Word count: " + _wordCount + " Syllable Count: " + _syllableCount;
    }

    public void setSentenceCount(int sentenceCount)
    {
        _sentenceCount.set(sentenceCount);
    }

    public void setWordCount(int wordCount)
    {
        _wordCount.set(wordCount);
    }

    public void setSyllableCount(int syllableCount)
    {
        _syllableCount.set(syllableCount);
    }

    public IntWritable getSentenceCount()
    {
        return _sentenceCount;
    }

    public IntWritable getWordCount()
    {
        return _wordCount;
    }

    public IntWritable getSyllableCount()
    {
        return _syllableCount;
    }
}
