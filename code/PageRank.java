package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PageRank {

	public static void main(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path tmp_outputPath = new Path(PageRankSetting.TMP_OUTPUT_PATH);
		double totalDeadEndScore;

		runParseInoutJob(inputPath, tmp_outputPath);
		totalDeadEndScore = getTotalDeadEndScore(tmp_outputPath, outputPath);
		System.out.println("Total Dead End Score = "+totalDeadEndScore);
	}

	private static void runParseInoutJob(Path input, Path output) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ParseInput");
		job.setJarByClass(PageRank.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(ParseInputMapper.class);
		job.setReducerClass(ParseInputReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(10);

		// add input/output path
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);


		job.waitForCompletion(true);

		Counter totalNodeCounter = job.getCounters().findCounter(NodeTypeCounter.TOTAL_NODE);
		Counter deadEndCounter = job.getCounters().findCounter(NodeTypeCounter.DEAD_END);
		long totalNodeCount = totalNodeCounter.getValue();
		long deadEndCount = deadEndCounter.getValue();

		System.out.println(totalNodeCounter.getDisplayName()+" = "+totalNodeCount);
		System.out.println(deadEndCounter.getDisplayName()+" = "+deadEndCount);
	}

	private static double getTotalDeadEndScore(Path input, Path output) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CalculateTotalDeadEndScore");
		job.setJarByClass(PageRank.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(DeadEndScoreSumMapper.class);
		job.setReducerClass(DeadEndScoreSumReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(BooleanWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		// set the number of reducer
		job.setNumReduceTasks(1);

		// add input/output path
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);

		double totalDeadEndScore;
		FileSystem fileSystem = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(
			new InputStreamReader(fileSystem.open(fileSystem.listStatus(output)[1].getPath()))
		);
		totalDeadEndScore = Double.parseDouble(reader.readLine());
		reader.close();
		fileSystem.close();

		return totalDeadEndScore;
	}
}
