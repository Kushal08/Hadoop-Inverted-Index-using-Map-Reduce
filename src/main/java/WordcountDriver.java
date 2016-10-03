import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordcountDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}
Job job = Job.getInstance(getConf());
job.setJobName("wordcount");

//Defining the output key and value class for the mapper

job.setMapOutputKeyClass(Text.class);

job.setMapOutputValueClass(Text.class);

job.setJarByClass(WordcountDriver.class);

job.setMapperClass(WordcountMapper.class);

job.setReducerClass(WordcountReducer.class);

//Defining the output value class for the mapper

job.setOutputKeyClass(Text.class);

job.setOutputValueClass(Text.class);

job.setInputFormatClass(TextInputFormat.class);

job.setOutputFormatClass(TextOutputFormat.class);

Path outputPath = new Path(args[1]);

FileInputFormat.addInputPath(job, new Path(args[0]));

FileOutputFormat.setOutputPath(job, outputPath);

/* Delete output filepath if already exists */
FileSystem fs = FileSystem.newInstance(getConf());

if (fs.exists(outputPath)) {
	fs.delete(outputPath, true);
}

return job.waitForCompletion(true) ? 0 : 1;
}

public static void main(String[] args) throws Exception {
WordcountDriver wordcountDriver = new WordcountDriver();
int res = ToolRunner.run(wordcountDriver, args);
System.exit(res);
}

}

