package tfidf.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import readability.util.FleschWritable;
import tfidf.util.TermFrequencyWritable;

/**
 * @author danbox
 * @date 4/16/14.
 */
public class TermFrequencyReducer extends Reducer<Text, TermFrequencyWritable, Text, DoubleWritable> {
}
