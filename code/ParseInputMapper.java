package pageRank;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ParseInputMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private StringBuilder outputBuffer = new StringBuilder();
	final static private Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
	final static private Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Matcher titleMatcher = titlePattern.matcher(line);
		Matcher linkMatcher = linkPattern.matcher(line);

		if ( titleMatcher.find() ){
			String title = PageRankUtils.replaceSpecialString(titleMatcher.group(1));
			outputKey.set(title);
			context.getCounter(NodeTypeCounter.TOTAL_NODE).increment(1);
			outputBuffer.setLength(0);
			while( linkMatcher.find() ){
				String link = PageRankUtils.replaceSpecialString(linkMatcher.group(1));
				outputBuffer.append("\t"+link);
			}
			if ( outputBuffer.length() == 0 ){
				context.getCounter(NodeTypeCounter.DEAD_END).increment(1);
			}
			outputValue.set(outputBuffer.toString());
			context.write(outputKey, outputValue);
		}
		else{
			throw new IOException("MYEXCEPTION: input doesn't have a title");
		}
	}



}
