package pageRank;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseInputReducer extends Reducer<Text,Text,Text,Text> {
	private Text outputValue = new Text();
	private double initScore = 0;
	private boolean isFirstKey;
	private HashSet<String> allTitles = new HashSet<String>();
	private StringBuilder buffer = new StringBuilder();
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		long totalNode = PageRankUtils.getCounter(context, NodeTypeCounter.TOTAL_NODE).getValue();
		initScore = 1.0/totalNode;
		isFirstKey = true;
		allTitles.clear();
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		if ( isFirstKey && !PageRankUtils.isSpecialKey(key.toString()) ){
			throw new IOException("MYERROR: Special key should be parsed first! ("+key.toString()+")");
		}
		else if ( !isFirstKey && PageRankUtils.isSpecialKey(key.toString())){
			throw new IOException("MYERROR: Special key should be pass to every reducer once!");
		}

		buffer.setLength(0);
		for (Text value : values){
			String link = value.toString();
			if ("".equals(link)){
				continue;
			}
			if ( isFirstKey ){
				allTitles.add(link);
			}
			else if( allTitles.contains(link) ){
				buffer.append("\t"+link);
			}
		}

		if (isFirstKey){
			isFirstKey = false;
		}
		else{
			outputValue.set(initScore + buffer.toString());
			context.write(key,outputValue);
		}
	}
}
