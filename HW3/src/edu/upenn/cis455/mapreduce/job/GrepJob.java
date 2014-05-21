package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class GrepJob implements Job{

  public void map(String key, String value, Context context)
  {
    	if(value.contains("ERROR")){
		context.write(key, value);
	}
	  
  }
  
  public void reduce(String key, String[] values, Context context)
  {
    	for(String value: values){
		context.write(key, value);
	}
	 
  }
  
}
