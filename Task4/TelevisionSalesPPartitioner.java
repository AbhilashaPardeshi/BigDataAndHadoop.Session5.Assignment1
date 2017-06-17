package Partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author abhilasha
 * Partitioner class that decides the partition based on the first letter of the company name
 *
 */
public class TelevisionSalesPPartitioner extends Partitioner<Text,IntWritable>
{

	@Override
	public int getPartition(Text key, IntWritable vale, int iNoOfPartitions) 
	{
		//Get company name
		String strCompanyName = key.toString();
		System.out.println("Current company name is to decide partition is : "+strCompanyName);
		
		//Extract first letter of the company name in upper case
		char cFirstLetter = strCompanyName.toUpperCase().charAt(0);
		
		//ASCII value comparison
		//A = 65, F = 70, G = 71, L = 76, M = 77, R = 82
		if(cFirstLetter >= 65 && cFirstLetter <= 70)
		{
			//First partition
			System.out.println("Company "+strCompanyName+" belongs to partition number 0");
			return 0;
		}
		else if(cFirstLetter >= 71 && cFirstLetter <= 76)
		{
			//Second partition
			System.out.println("Company "+strCompanyName+" belongs to partition number 1");
			return 1;
		}
		else if(cFirstLetter >= 77 && cFirstLetter <= 82)
		{
			//Third partition
			System.out.println("Company "+strCompanyName+" belongs to partition number 2");
			return 2;
		}
		
		//Fourth partition
		System.out.println("Company "+strCompanyName+" belongs to partition number 3");
		return 3;
		
	}

}
