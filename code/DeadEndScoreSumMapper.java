package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class DeadEndScoreSumMapper extends Mapper<LongWritable, Text, BooleanWritable, DoubleWritable> {
	final static private BooleanWritable outputKey = new BooleanWritable(true);
	private DoubleWritable outputValue = new DoubleWritable();
	private double deadEndScore;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		deadEndScore = 0;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] token = value.toString().split("\t",3);

		if ( token.length == 2 ){
			deadEndScore += Double.parseDouble(token[1]);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		outputValue.set(deadEndScore);
		context.write(outputKey, outputValue);
	}

}
