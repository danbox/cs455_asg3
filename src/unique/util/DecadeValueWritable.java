package unique.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author danbox
 * @date 4/27/14.
 */
public class DecadeValueWritable implements Writable
{
    private IntWritable     _decade;
    private DoubleWritable  _val;

    public DecadeValueWritable()
    {
        _decade = new IntWritable();
        _val = new DoubleWritable();
    }

    public DecadeValueWritable(IntWritable decade, DoubleWritable val)
    {
        _decade = decade;
        _val = val;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        _decade.write(dataOutput);
        _val.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        _decade.readFields(dataInput);
        _val.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return _decade + " " + _val;
    }

    public void setDecade(int decade)
    {
        _decade.set(decade);
    }

    public void setVal(double val)
    {
        _val.set(val);
    }


    public IntWritable getDecade()
    {
        return _decade;
    }

    public DoubleWritable getVal()
    {
        return _val;
    }

}

