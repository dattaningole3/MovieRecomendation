package Acadgild.MovieRatingAssignment;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MoviesReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Context context)
			throws IOException, InterruptedException {
		String titles = "";
		double total = 0.0;
		int count = 0;
		
		for(Text t:arg1) {
			String parts[] = t.toString().split("\t");
			if (parts[0].equals("ratings")) {
				count++;
				String rating = parts[1].trim();
//				System.out.println("Rating is =>"+rating);
				total += Double.parseDouble(rating);
			} else if (parts[0].equals("movies")) {
				titles = parts[1];
			}
		}
		
		double average = total / count;
		String str = String.format("%d\t%f", count, average);
		context.write(new Text(titles), new Text(str));
	}
}
