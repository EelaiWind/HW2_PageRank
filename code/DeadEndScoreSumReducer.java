package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class DeadEndScoreSumReducer extends Reducer<BooleanWritable,DoubleWritable,NullWritable,DoubleWritable> {
	private DoubleWritable outputValue = new DoubleWritable();
	private double initScore = -1;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		long totalNode = PageRankUtils.getCounter(context, NodeTypeCounter.TOTAL_NODE).getValue();
		initScore = 1.0/totalNode;
	}

	public void reduce(BooleanWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double deadEndScore = 0;
		for (DoubleWritable score : values){
			deadEndScore += score.get();
		}
		outputValue.set(deadEndScore);
		context.write(NullWritable.get(), outputValue);
	}
}
