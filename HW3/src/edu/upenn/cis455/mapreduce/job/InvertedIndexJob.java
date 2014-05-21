package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class InvertedIndexJob implements Job{

  public void map(String key, String value, Context context)
  {
  	String [] words = value.split(" ");
    	for(String word: words){
		if(word.equals("one") || word.equals("two") || word.equals("three") || word.equals("four") || word.equals("five") || word.equals("six") || word.equals("seven")){
			context.write(word, key);
		}
	}
        
	  
  }
  
  public void reduce(String key, String[] values, Context context)
  {
  	StringBuffer sb = new StringBuffer();
        sb.append("||");
    	for(String value: values){
		sb.append(value+"||");
	}
        context.write(key, sb.toString());
	 
  }
  
}
