package mapreducetests;


import java.security.NoSuchAlgorithmException;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.job.MyContext;
import edu.upenn.cis455.mapreduce.job.WordCount;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class Test {

	@org.junit.Test
	public void test() throws NoSuchAlgorithmException {
		WordCount wc = new WordCount();
		Context c=null;
		wc.map("a", "b", c);
	}
	
	@org.junit.Test
	public void test3() throws NoSuchAlgorithmException {
		WordCount wc = new WordCount();
		Context c=null;
		wc.map("1", "a b c d", c);
	}
	
	@org.junit.Test
	public void test2() throws NoSuchAlgorithmException {
		WorkerServlet w = new WorkerServlet();
		w.ws = w.new WorkerStatus();
		
		w.workers.put("worker1", "127.0.0.1:2000");
		w.workers.put("worker2", "127.0.0.1:3000");

		w.determineMyId();
	}
	
	@org.junit.Test
	public void test4() throws NoSuchAlgorithmException {
		MyContext c = new MyContext(5,null,1, null);
		c.write("a", "1");
		c.write("asga", "1");
		c.write("adfhdfna", "1");
		c.write("fsda", "1");
		c.write("adfnadfna", "1");
		c.write("m,ndta", "1");
		c.write("ih23a", "1");

	}
	

}
