package edu.upenn.cis455.mapreduce.job;

import java.util.StringTokenizer;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job{

	public void map(String key, String value, Context context)
	{
		// Your map function for WordCount goes here
		StringTokenizer st = new StringTokenizer(value);
		while(st.hasMoreTokens()) {
			String k = st.nextToken();
			String v = "1";
			context.write(k, v);
		}
	}

	public void reduce(String key, String[] values, Context context)
	{
		try{
			if(key.equals("")||values.equals(null))
				return;
			int count = 0;
			for(String v:values){
				count += Integer.parseInt(v);
			}
			context.write(key,Integer.toString(count));
		}
		catch(NumberFormatException e) {
			System.out.println("Exception caught");
		}
	}

}
