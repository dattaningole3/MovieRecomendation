package Acadgild.MovieRatingAssignment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieDriver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.err.println("Usage: MovieRatingUseCases <input path1> <input path2> <output path>");
			System.exit(-1);
		}

		// Job Related Configurations
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MovieRatingUseCases");
		job.setJarByClass(MovieDriver.class);

		// job.setNumReduceTasks(0);

		// Since there are multiple input, there is a slightly different way of
		// specifying input path, input format and mapper
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MoviesMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingsMapper.class);

		// Set the reducer
		job.setReducerClass(MoviesReducer.class);

		// set the out path
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		// set up the output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// execute the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
