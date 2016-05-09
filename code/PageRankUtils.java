package pageRank;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;

public class PageRankUtils{

	public static Counter getCounter(JobContext context, NodeTypeCounter type) throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		return currentJob.getCounters().findCounter(NodeTypeCounter.TOTAL_NODE);
	}

	public static String replaceSpecialString(String input){
		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "'");
	}

}