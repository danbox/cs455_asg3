package tfidf.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
    private Text        _term;
    private IntWritable _frequency;

    public TermFrequencyWritable()
    {
        _term = new Text();
        _frequency = new IntWritable();
    }

    public TermFrequencyWritable(String term, int frequency)
    {
        _term = new Text(term);
        _frequency = new IntWritable(frequency);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        _term.write(dataOutput);
        _frequency.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        _term.readFields(dataInput);
        _frequency.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return "Term: " + _term + " Frequency: " + _frequency;
    }

    public Text getTerm()
    {
        return _term;
    }

    public IntWritable getFrequency()
    {
        return _frequency;
    }

    public void setTerm(String term)
    {
        _term.set(term);
    }

    public void setFrequency(int frequency)
    {
        _frequency.set(frequency);
    }
}
