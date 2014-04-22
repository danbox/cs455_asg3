package ngram.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author danbox
 * @date 4/21/14.
 */

public class NGramWritable implements WritableComparable<NGramWritable>
{
    private Text _filename;
    private Text _term;

    public NGramWritable()
    {
        _filename = new Text();
        _term = new Text();
    }

    public NGramWritable(String filename, String term)
    {
        _filename = new Text(filename);
        _term = new Text(term);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        _filename.write(dataOutput);
        _term.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        _filename.readFields(dataInput);
        _term.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return "filename: " + _filename + " term: " + _term;
    }

    @Override
    public int compareTo(NGramWritable other)
    {
        if(_filename.compareTo(other._filename) == 0 && _term.compareTo(other._term) == 0)
        {
            return 0;
        }
        else return _term.compareTo(other._term);
    }

    @Override
    public int hashCode()
    {
        return _term.hashCode();
    }

    public Text getfilename()
    {
        return _filename;
    }

    public Text getterm()
    {
        return _term;
    }

    public void setfilename(String filename)
    {
        _filename.set(filename);
    }

    public void setterm(String term)
    {
        _term.set(term);
    }
}

