package unique.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class TermValueWritable implements Writable
{
    private Text     _term;
    private DoubleWritable  _val;

    public TermValueWritable()
    {
        _term = new Text();
        _val = new DoubleWritable();
    }

    public TermValueWritable(Text term, DoubleWritable val)
    {
        _term = term;
        _val = val;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        _term.write(dataOutput);
        _val.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        _term.readFields(dataInput);
        _val.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return _term + " " + _val;
    }

    public void setTerm(String term)
    {
        _term.set(term);
    }

    public void setVal(double val)
    {
        _val.set(val);
    }


    public Text getDecade()
    {
        return _term;
    }

    public DoubleWritable getVal()
    {
        return _val;
    }
}
