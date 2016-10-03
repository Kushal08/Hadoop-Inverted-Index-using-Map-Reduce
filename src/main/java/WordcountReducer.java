import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.File;
import java.util.Scanner;

public class WordcountReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {

		HashMap m=new HashMap();
		int count=0;


		for(Text t:values)
		{
		String str=t.toString();

		/*Check if file name is present in the HashMap ,if File name is not present then add the Filename to the HashMap and increment the counter by one , This condition will be satisfied on first occurrence of that word*/
		if(m!=null && m.get(str)!=null)
		{
		count=(int)m.get(str);
		m.put(str, ++count);
		}
		else
		{
		/*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
		m.put(str, 1);
		}
		}

		context.write(key, new Text(m.toString()));

	}

}
