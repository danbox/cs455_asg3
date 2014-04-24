package tfidf.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class TermFrequencyWritable implements Writable
{
    private IntWritable _maxFrequency;
    private IntWritable _frequency;

    public TermFrequencyWritable()
    {
        _maxFrequency = new IntWritable();
        _frequency = new IntWritable();
    }

    public TermFrequencyWritable(int frequency, int maxFrequency)
    {
        _maxFrequency = new IntWritable(maxFrequency);
        _frequency = new IntWritable(frequency);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        _maxFrequency.write(dataOutput);
        _frequency.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        _maxFrequency.readFields(dataInput);
        _frequency.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return "MaxFrequency: " + _maxFrequency + " Frequency: " + _frequency;
    }

    public IntWritable getMaxFrequency()
    {
        return _maxFrequency;
    }

    public IntWritable getFrequency()
    {
        return _frequency;
    }

    public void setMaxFrequency(int maxFrequency)
    {
        _maxFrequency.set(maxFrequency);
    }

    public void setFrequency(int frequency)
    {
        _frequency.set(frequency);
    }
}
