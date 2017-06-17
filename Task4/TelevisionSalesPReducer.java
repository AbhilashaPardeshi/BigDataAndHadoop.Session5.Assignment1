package Partitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author abhilasha
 * Input key is the the name of the company
 * Input value is 1
 * Output key is the company name
 * OUtput value is the count of televisions for a company
 *
 */
public class TelevisionSalesPReducer extends Reducer<Text,IntWritable,Text,LongWritable>
{
	LongWritable count;
	
	@Override
	public void setup(Context context)
	{
		count = new LongWritable();
	}
	
	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		long lCount = 0;
		for(IntWritable value:values)
		{
			lCount=lCount + value.get();
		}
		count.set(lCount);
		context.write(key, count);
	}
}
