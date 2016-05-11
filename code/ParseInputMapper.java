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
	private long localTotalNodeCount;
	final static private Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
	final static private Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		localTotalNodeCount = 0;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Matcher titleMatcher = titlePattern.matcher(line);
		Matcher linkMatcher = linkPattern.matcher(line);

		if ( titleMatcher.find() ){
			String title = replaceSpecialString(titleMatcher.group(1));
			title = capitalizeFirstLetter(title);
			localTotalNodeCount += 1;
			// pass deadend information
			outputKey.set(title);
			outputValue.set("");
			context.write(outputKey, outputValue);

			while( linkMatcher.find() ){
				String link = replaceSpecialString(linkMatcher.group(1));
				link = capitalizeFirstLetter(link);
				outputValue.set(link);
				context.write(outputKey, outputValue);
			}

			outputValue.set(title);
			for (int i = 0; i < PageRankSetting.PARSE_INPUT_REDUCER_COUNT; i++){
				outputKey.set(" "+i+"<");
				context.write(outputKey, outputValue);
			}
		}
		else{
			throw new IOException("MYERROR: input doesn't have a title");
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		context.getCounter(NodeTypeCounter.TOTAL_NODE).increment(localTotalNodeCount);
	}

	private String replaceSpecialString(String input){
		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "'");
	}

	private String capitalizeFirstLetter(String input){
		char firstChar = input.charAt(0);
		if ( (firstChar >= 'a' && firstChar <='z') || (firstChar>= 'A' && firstChar <= 'Z') ){
			if ( input.length() == 1 ){
				return input.toUpperCase();
			}
			else{
				return input.substring(0, 1).toUpperCase() + input.substring(1);
			}
		}
		else{
			return input;
		}
	}

}
