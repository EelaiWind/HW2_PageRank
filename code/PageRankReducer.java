package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducer extends Reducer<Text,ScoreNeighborNodesPair,Text,ScoreNeighborNodesPair> {
	private ScoreNeighborNodesPair outputValue = new ScoreNeighborNodesPair();
	private int totalNodeCount;
	private double alpha;
	private double totalDeadEndScoreSum;
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		totalNodeCount = context.getConfiguration().getInt(PageRankSetting.TOTAL_NODE_COUNT_KEY,-1);
		if (totalNodeCount == -1){
			throw new IOException("MYECEPTION: "+PageRankSetting.TOTAL_NODE_COUNT_KEY+" is not set in configuration");
		}

		totalDeadEndScoreSum = context.getConfiguration().getInt(PageRankSetting.TOTAL_DEAD_END_SCORE_KEY,-1);
		if (totalNodeCount == -1){
			throw new IOException("MYECEPTION: "+PageRankSetting.TOTAL_DEAD_END_SCORE_KEY+" is not set in configuration");
		}

		alpha = PageRankSetting.ALPHA;
	}

	public void reduce(Text key, Iterable<ScoreNeighborNodesPair> values, Context context) throws IOException, InterruptedException {
		double newPageRank = 0;
		for ( ScoreNeighborNodesPair value : values ){
			double score = value.getScore();
			if ( score == PageRankSetting.IGNORE_THIS_SCORE ){
				outputValue.setNeighborNiodes(value.getNeighborNodes());
			}
			else{
				newPageRank += score;
			}
		}
		outputValue.setScore(newPageRank);
		context.write(key, outputValue);
	}
}
