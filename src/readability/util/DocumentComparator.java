package readability.util;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;

/**
 * @author danbox
 * @date 4/24/14.
 */
public class DocumentComparator extends WritableComparator
{
    DataInputBuffer _dataInputBuffer;
    Text            _text1;
    Text            _text2;

    public DocumentComparator()
    {
        super(Text.class);
        _dataInputBuffer = new DataInputBuffer();
        _text1 = new Text();
        _text2 = new Text();
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            _dataInputBuffer.reset(b1, s1, l1);                   // parse key1
            _text1.readFields(_dataInputBuffer);

            _dataInputBuffer.reset(b2, s2, l2);                   // parse key2
            _text2.readFields(_dataInputBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return compare(_text1, _text2);                   // compare them
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b)
    {
        Text text1 = (Text)a;
        Text text2 = (Text)b;

        StringBuilder stringBuilder1 = new StringBuilder();
        StringBuilder stringBuilder2 = new StringBuilder();


        if(Character.isDigit(text1.toString().charAt(11)))
        {
            stringBuilder1.append(text1.toString().substring(8, 12));
        } else
        {
            stringBuilder1.append(0);
            stringBuilder1.append(text1.toString().substring(8, 11));
        }

        if(Character.isDigit(text2.toString().charAt(11)))
        {
            stringBuilder2.append(text2.toString().substring(8, 12));
        } else
        {
            stringBuilder2.append(0);
            stringBuilder2.append(text2.toString().substring(8, 11));
        }

        stringBuilder1.append(text1.toString().substring(0,3));
        stringBuilder2.append(text2.toString().substring(0,3));


        return stringBuilder1.toString().compareTo(stringBuilder2.toString());
    }
}
