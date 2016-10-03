import java.io.IOException;

import java.util.StringTokenizer;
import java.io.File;
import java.util.Scanner;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class WordcountMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();
	private Text filename = new Text();

	private boolean caseSensitive = false;
    
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		char[] w = new char[501];
		PorterStemmer ste = new PorterStemmer();
		HashMap<String, Integer> stopword =new HashMap<String, Integer>();

		File dir1 = new File("/home/kushal/Desk/ir-labs/lab1/src/dirs/Stopword.txt");
    String str1= new Scanner(dir1).useDelimiter("\\Z").next();
		String[] stpword2=str1.split("\n");
    for(String stp:stpword2)
		{
			stp = stp.trim();
			stopword.put(stp,1);
		}

		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		String line=value.toString();
		//Split the line in words
		String words[]=line.split(" ");
		int count = 0;
		for(String s1:words)
		{

		     for (int xx = 0; xx < s1.length(); xx++) 
		    	 w[xx] = s1.charAt(xx);
		      
		     for (int c = 0; c < s1.length(); c++) 
		    	    ste.add(Character.toLowerCase(w[c]));
		            ste.stem();
		            s1 = ste.toString();
		            words[count] = s1;
		            count = count+1;
		            ste.reset();
		             
		}
		for(String s:words)
		{
		if(!stopword.containsKey(s))
		{
		//for each word emit word as key and file name as value
		context.write(new Text(s), new Text(fileName));
	  }
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.caseSensitive = conf.getBoolean("wordcount.case.sensitive",false);
	}
}


