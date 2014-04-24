package ngram.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @author danbox
 * @date 4/21/14.
 */

public class NGramWritableComparable implements WritableComparable<NGramWritableComparable>
{
    private String _filename;
    private String _term;

    public NGramWritableComparable()
    {
        super();
        _filename = new String();
        _term = new String();
    }

    public NGramWritableComparable(String filename, String term)
    {
        super();
        _filename = new String(filename);
        _term = new String(term);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(_filename);
        dataOutput.writeUTF(_term);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        _filename = dataInput.readUTF();
        _term = dataInput.readUTF();
    }

    @Override
    public String toString()
    {
        return _filename + " " + _term;
    }

    @Override
    public int compareTo(NGramWritableComparable o)
    {
        return (_filename.compareTo(o._filename) != 0) ? _filename
                .compareTo(o._filename) : _term
                .compareTo(o._term);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(_filename.toString());
    }

    @Override
    public boolean equals(final Object obj)
    {
        if(obj instanceof NGramWritableComparable)
        {
            final NGramWritableComparable o = (NGramWritableComparable)obj;
            return Objects.equals(_filename, o._filename) &&
                    Objects.equals(_term, o._term);
        }
        return false;
    }

    public String getFilename()
    {
        return _filename;
    }

    public String getTerm()
    {
        return _term;
    }

    public void setfilename(String filename)
    {
        _filename = filename;
    }

    public void setterm(String term)
    {
        _term = term;
    }
}

