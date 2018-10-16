package Acadgild.MovieRatingAssignment;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
		try {
			if (key.get()==0 && value.toString().contains("userId")) {
				return;
			} else {
				String record = value.toString();
				String[] parts = record.split(",");
				context.write(new Text(parts[1]), new Text("ratings\t"+parts[2]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
