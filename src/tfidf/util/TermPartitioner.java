package tfidf.util;

import ngram.util.NGramWritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author danbox
 * @date 4/23/14.
 */
public class TermPartitioner extends Partitioner<NGramWritableComparable, IntWritable>
{
    @Override
    public int getPartition(NGramWritableComparable key, IntWritable value, int numPartitions)
    {
        int hash = key.getFilename().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }
}
