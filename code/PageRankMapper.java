package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class PageRankMapper extends Mapper<LongWritable, Text, Text, ScoreNeighborNodesPair> {
	private Text outputKey = new Text();
	private ScoreNeighborNodesPair outputValue = new ScoreNeighborNodesPair();
	private int totalNodeCount;
	private double alpha;
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		totalNodeCount = context.getConfiguration().getInt(PageRankSetting.TOTAL_NODE_COUNT_KEY,-1);
		if (totalNodeCount == -1){
			throw new IOException("MYECEPTION: "+PageRankSetting.TOTAL_NODE_COUNT_KEY+" is not set in configuration");
		}

		alpha = PageRankSetting.ALPHA;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] token = value.toString().split("\t",3);

		if ( token.length == 3 ){
			double sourceScore = Double.parseDouble(token[2]);
			String[] neighborNodes = token[2].split("\t");
			int outDegree = neighborNodes.length;
			double score  = alpha*sourceScore/outDegree;
			for (String node : neighborNodes){
				distributePageRank(context, node, score);
			}
			passLinkStructure(context, token[0], token[2]);
		}
		else if ( token.length != 2 ){
			throw new IOException("MYEXCEPTION: There are unexpected tab(\\t) in links");
		}
	}

	private void passLinkStructure(Context context, String source, String neighborNodes)throws IOException, InterruptedException{
		outputKey.set(source);
		outputValue.setScore(PageRankSetting.IGNORE_THIS_SCORE);
		outputValue.setNeighborNiodes(neighborNodes);
		context.write(outputKey,outputValue);
	}

	private void distributePageRank(Context context, String neighborNode, double score) throws IOException, InterruptedException{
		outputKey.set(neighborNode);
		outputValue.setScore(score);
		outputValue.setNeighborNiodes("");
		context.write(outputKey,outputValue);
	}
}
