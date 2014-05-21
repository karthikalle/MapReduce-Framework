package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MyContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.*;
import javax.servlet.http.*;

public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;
	public WorkerStatus ws;
	BlockingQueue<String> fileQueue;
	BlockingQueue<String> lineQueue;
	String master;
	HashMap<String, String> runMapParams = new HashMap<String, String>();
	HashMap<String, String> runReduceParams = new HashMap<String, String>();

	public HashMap<String, String> workers = new HashMap<String, String>();
	int myId;
	MyContext context;
	int numWorkers;
	int i;

	public class WorkerStatus {
		int port;
		public String status;
		String job;
		int keysRead;
		public int keysWritten;
	}

	public class PostThreads extends Thread {
		String url, urlParameters;
		public PostThreads(String u, String p) {
			url = u;
			urlParameters = p;
		}
		@Override
		public void run() {
			int repeatFlag = 1;
			URL master;
			try {
				while(repeatFlag==1) {
					master = new URL(url);

					HttpURLConnection conn = (HttpURLConnection) master.openConnection();

					conn.setDoOutput(true);
					conn.setRequestMethod("POST");
					conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); 
					conn.setRequestProperty("charset", "utf-8");
					conn.setRequestProperty("Content-Length", "" + Integer.toString(urlParameters.getBytes().length));


					conn.setDoOutput(true);
					DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
					wr.writeBytes(urlParameters);
					wr.flush();
					wr.close();
					int respCode = (conn.getResponseCode());

					System.out.println(respCode+ " for url "+ url);
					if(respCode != 200) {
						conn.disconnect();
						continue;
					}
					else
						repeatFlag = 0;
					conn.setReadTimeout(2*2000);

					BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));

					String line;
					while ((line = reader.readLine()) != null) {
						System.out.println(line);
					}
					reader.close();
					conn.disconnect();
				}

			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				//e.printStackTrace();
			}

		}
	}

	public class MyThreads extends Thread {
		boolean runFlag = true;
		@Override
		public void run() {

			while(runFlag) {
				String line;
				try {
					synchronized(lineQueue) {
						while(lineQueue.size()==0 && runFlag) 
							lineQueue.wait();
						if(!runFlag)
							continue;
						line = lineQueue.remove();
					}
				//	System.out.println("Thread: "+this.getId()+" read: "+line);
					try {
						Job job = (Job) Class.forName(runMapParams.get("job")).newInstance();
						if(ws.status.equals("mapping")) {
							String args[] = line.split("\t");
							if(args.length==2) {
								job.map(args[0], args[1], context);
								ws.keysRead++;
							}
						}
						else {
							String[] lines = line.split(System.getProperty("line.separator"));
							String values[] = new String[lines.length];
							for(int i = 0; i<lines.length; i++) {
								if(lines[i].split("\t").length==2)
									values[i]=(lines[i].split("\t")[1]);
							}
							/*System.out.println(this.getId()+" Key is: "+lines[0].split("\t")[0]);
							System.out.println(this.getId()+" values are:");
							for(String v:values){
								System.out.print(this.getId()+" "+v);
							}
							System.out.println("\n");
							 */		
							if(lines.length==0)
								continue;
							job.reduce(lines[0].split("\t")[0], values, context);
							ws.keysRead ++;
						}

					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					} catch (InstantiationException e) {
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					}
				} catch (InterruptedException e1) {
					//e1.printStackTrace();
					runFlag = false;
				}
			}
		}
	}

	public class StatusThread extends Thread {
		boolean alwaysFlag;
		public StatusThread(boolean flag) {
			alwaysFlag = flag;
		}
		public void run() {
			do {
				try {
					String url = "http://"+master+""
							+ "/workerstatus?"
							+ "port="+ws.port+"&status="+ws.status+"&job="+
							ws.job+"&keysRead="+ws.keysRead+"&keysWritten="+ws.keysWritten;
					URL master = new URL(url);
					HttpURLConnection conn = (HttpURLConnection) master.openConnection();

					conn.setRequestProperty("User-Agent", "localhost");
					conn.setRequestMethod("GET");
					conn.setDoOutput(true);
					conn.getResponseCode();
					//System.out.println();
					conn.disconnect();
					sleep(10000);

				} catch (Exception e) {
					continue;
				}
			} while(alwaysFlag);
		}
	}


	public void init(final ServletConfig config) throws ServletException {
		System.out.println("Worker initialized");
		ws = new WorkerStatus();
		ws.port = Integer.parseInt(config.getInitParameter("listeningPort"));
		ws.status = "idle";
		ws.keysRead = 0;
		ws.keysWritten = 0;
		ws.job = "null";

		runMapParams.put("storagedir", config.getInitParameter("storagedir"));

		master = config.getInitParameter("master");
		Thread t = new StatusThread(true);
		t.start();
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		String workType = request.getRequestURI();
		switch(workType) {
		case "/runmap":
			handleRunMap(request,response);
			break;
		case "/runreduce":
			handleRunReduce(request,response);
			break;
		case "/pushdata":
			//System.out.println("pushdata request");
			handlePushData(request,response);
			break;
		}
		System.out.println("\n"+workType +" work done for worker"+myId );
	}

	private void handlePushData(HttpServletRequest request,	HttpServletResponse response) {
		String content = request.getParameter("content");
		//context = new MyContext(runMapParams.get("storagedir"), ws);
	//	System.out.println(context!=null);
		if(context!=null) {
			System.out.println("Got push data "+i+" from worker"+request.getParameter("from"));
			i++;
			context.writeToSpoolIn(myId, content);
		}
		if(i==numWorkers) {
			ws.status="waiting";
			Thread t = new StatusThread(false);
			t.start();
		}


	}

	private void handleRunReduce(HttpServletRequest request, HttpServletResponse response) {
		ws.status="reducing";
		System.out.println("Inside run reduce");

		ws.keysRead = 0;
		ws.keysWritten = 0;
		runReduceParams.put("job",request.getParameter("job"));
		runReduceParams.put("output",request.getParameter("output"));
		runReduceParams.put("numThreads",request.getParameter("numThreads"));

		ws.job = runReduceParams.get("job");

		Thread t = new StatusThread(false);
		t.start();

		context = new MyContext(runMapParams.get("storagedir"), ws, runReduceParams.get("output"));
		performReduce();
	}

	private void performReduce() {
		context.sortToReduce();
		ArrayList<MyThreads> myThreads = new ArrayList<MyThreads>();
	//	System.out.println("number of reduce threads:"+runReduceParams.get("numThreads"));
		for(int i = 0; i<Integer.parseInt(runReduceParams.get("numThreads")); i++) {
			myThreads.add(new MyThreads());
			myThreads.get(i).start();
		}

		try {
			BufferedReader br = new BufferedReader(new FileReader(runMapParams.get("storagedir")+"spoolin/reduce.txt"));
			String line, oldKey="", presentKey, value = "";
			while((line = br.readLine())!=null) {
				presentKey = line.split("\t")[0];
				//	System.out.println("oldkey: "+oldKey +" presentkey: "+presentKey);
				if(oldKey.equals(presentKey)) {
					value += line+"\n";
				}
				else {
					synchronized(lineQueue) {
						lineQueue.add(value);
						//System.out.println("Adding to linequeue:\n"+value);
						lineQueue.notify();
					}
					value = line+"\n";
					oldKey = presentKey;
				}
			}
			synchronized(lineQueue) {
				lineQueue.add(value);
			//	System.out.println("Adding to linequeue:\n"+value);
				lineQueue.notify();
			}

			br.close();
			while(lineQueue.size()!=0){}

			for(MyThreads myT: myThreads) {
				myT.interrupt();
			}

			for(MyThreads myT: myThreads) {
				while(myT.isAlive()){
					//	System.out.println(myT.getId()+" is still alive");
				}
			}
			ws.status="idle";
			ws.keysRead = 0;
			ws.job = "null";
			Thread t = new StatusThread(false);
			t.start();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void handleRunMap(HttpServletRequest request, HttpServletResponse response) throws UnsupportedEncodingException {
		i = 0;
		ws.keysRead = 0;
		ws.keysWritten = 0;
		fileQueue = new LinkedBlockingQueue<String>();
		lineQueue = new LinkedBlockingQueue<String>();

		System.out.println("Inside run map");
		runMapParams.put("job",request.getParameter("job"));
		String input = URLDecoder.decode(request.getParameter("input"), "UTF-8");
		runMapParams.put("input",input);

		runMapParams.put("numThreads",request.getParameter("numThreads"));
		runMapParams.put("numWorkers",request.getParameter("numWorkers"));

		numWorkers = Integer.parseInt(runMapParams.get("numWorkers"));
		for(int i =0; i<Integer.parseInt(runMapParams.get("numWorkers"));i++){
			System.out.println("Putting into workers:"+request.getParameter("worker"+i)+" worker"+i);
			workers.put("worker"+i, request.getParameter("worker"+i));

		}
		ws.job = runMapParams.get("job");

		ws.status = "mapping";
		Thread t = new StatusThread(false);
		t.start();

		determineMyId();		
		context = new MyContext(Integer.parseInt(runMapParams.get("numWorkers")), runMapParams.get("storagedir"), myId, ws);

		performMap();
	}

	public void determineMyId() {
		for(String w: workers.keySet()) {
			if(workers.get(w).contains(Integer.toString(ws.port))) {
				myId = Integer.parseInt(w.split("worker")[1]);
			}
		}
	}

	private void performPushData() {
		HashMap<String, String> workerPushData = context.getPushData();

		for(String w:workers.keySet()) {
			System.out.println("Worker: "+w+" "+workers.get(w));
		}

		for(String worker: workerPushData.keySet()) {
			String url = "http://"+workers.get(worker)+"/pushdata";
			System.out.println(url);
			String urlParameters = "";
			urlParameters = "content="+workerPushData.get(worker)+"&from="+myId;
			
			PostThreads m = new PostThreads(url, urlParameters);
			m.start();

		}
	}


	private void performMap() {
		String inputDirPath = runMapParams.get("storagedir")+runMapParams.get("input");
		System.out.println("Input dir:"+inputDirPath);

		File f = new File(inputDirPath);
		ArrayList<MyThreads> myThreads = new ArrayList<MyThreads>();
		//System.out.println("number of map threads:"+runMapParams.get("numThreads"));

		for(int i = 0; i<Integer.parseInt(runMapParams.get("numThreads")); i++) {
			myThreads.add(new MyThreads());
			myThreads.get(i).start();
		}

		for(File file :f.listFiles()) {
			try {
				if(file.getName().equals(".DS_Store")||file.getName().equals(".svn")) {
					continue;
				}
				BufferedReader br = new BufferedReader(new FileReader(file));

				String line;

				while((line = br.readLine())!=null) {
					synchronized(lineQueue) {
						//	System.out.println("Worker putting into queue: "+line);
						lineQueue.add(line);
						lineQueue.notify();
					}
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		while(lineQueue.size()!=0){}

		for(MyThreads myT: myThreads) {
			myT.interrupt();
		}

		for(MyThreads myT: myThreads) {
			while(myT.isAlive()){
				//System.out.println(myT.getId()+" is still alive");
			}
		}

		performPushData();
		while(!ws.status.equals("waiting")) {
		//	System.out.println("wait to get all data");
		}
		/*		ws.status="waiting";
		Thread t = new StatusThread(false);
		t.start();
		 */	
	}

}

