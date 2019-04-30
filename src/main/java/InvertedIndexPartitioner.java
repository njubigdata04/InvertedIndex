import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        //key: word#filename

        String authorName = NovelNameProcess.splitAuthorName(key.toString().split("#")[1]);

        return super.getPartition(new Text(authorName), value, numReduceTasks);
    }
}
