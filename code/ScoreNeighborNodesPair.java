package pageRank;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashSet;
import java.util.Collection;

public class ScoreNeighborNodesPair implements Writable{
	private double score;
	private Text neighborNodes;

	public ScoreNeighborNodesPair(){
		score = 0;
		neighborNodes = new Text();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(score);
		neighborNodes.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		score = in.readDouble();
		neighborNodes.readFields(in);
	}

	public void setScore(double score){
		this.score = score;
	}

	public void setNeighborNiodes(Text neighborNodes){
		this.neighborNodes = neighborNodes;
	}

	public void setNeighborNiodes(String neighborNodes){
		this.neighborNodes.set(neighborNodes);
	}

	public double getScore(){
		return this.score;
	}

	public Text getNeighborNodes(){
		return this.neighborNodes;
	}

	public String toString(){
		return score+" "+neighborNodes;
	}

}