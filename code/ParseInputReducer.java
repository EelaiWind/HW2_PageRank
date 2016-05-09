package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ParseInputReducer extends Reducer<Text,Text,Text,Text> {
	private Text outputValue = new Text();
	private double initScore = -1;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		long totalNode = PageRankUtils.getCounter(context, NodeTypeCounter.TOTAL_NODE).getValue();
		initScore = 1.0/totalNode;
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean isFirst = true;
		for (Text value : values){
			if ( isFirst ){
				isFirst = false;
				outputValue.set(initScore+" "+value.toString());
				context.write(key, outputValue);
			}
			else{
				throw new IOException("MYEXCEPTION: same title in more than 2 lines");
			}
		}
	}
}
