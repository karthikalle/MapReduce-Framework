package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

	static final long serialVersionUID = 455555001;
	public class WorkerStatus {
		int port;
		String status;
		String job;
		int keysRead;
		int keysWritten;
	}

	int syncflag = 0;

	HashMap<Integer, WorkerStatus> workers = new HashMap<Integer, WorkerStatus>();
	HashMap<Integer, Long> lastReceived = new HashMap<Integer, Long>();
	HashMap<String, String> runMapParams = new HashMap<String, String>();
	HashMap<String, Long> lastReceivedWithIP = new HashMap<String, Long>();

	public class MasterThreads extends Thread {
		String url, urlParameters;
		public MasterThreads(String u, String p) {
			url = u;
			urlParameters = p;
		}
		@Override
		public void run() {
			URL master;
			try {
				master = new URL(url);
				//	System.out.println(url);
				HttpURLConnection conn = (HttpURLConnection) master.openConnection();

				conn.setDoOutput(true);
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); 
				conn.setRequestProperty("charset", "utf-8");
				conn.setRequestProperty("Content-Length", "" + Integer.toString(urlParameters.getBytes().length));



				conn.setDoOutput(true);
				DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
				//	System.out.println(urlParameters.substring(0,urlParameters.length()-1));
				wr.writeBytes(urlParameters);
				wr.flush();
				wr.close();
				BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));

				String line;
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
				}

				conn.disconnect();

			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				//e.printStackTrace();


			}

		}
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		String workType = request.getRequestURI();
		//String workType = config.getInitParameter("workType");
		switch(workType) {
		case "/workerstatus":
			handleWorkerStatus(request,response);
			break;
		case "/status":
			handleStatus(request,response);
			break;
		}

	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		String workType = request.getRequestURI();
		//String workType = config.getInitParameter("workType");
		//	System.out.println(workType);
		switch(workType) {
		case "/sendRunMap":
			handleSendRunmap(request, response);
			break;
		}

	}

	private synchronized void handleStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		//String IPAddress = (String) request.getAttribute("IPAddress");

		out.println("<html><head><title>Status Page</title></head><body>");
		out.println("<table><tr><th>Worker&nbsp;&nbsp;&nbsp;</th><th>Status&nbsp;&nbsp;&nbsp;</th><th>Job&nbsp;&nbsp;&nbsp;</th>"
				+ "<th>KeysRead&nbsp;&nbsp;&nbsp;</th><th>KeysWritten&nbsp;&nbsp;&nbsp;</th></tr>");

		for(int p: workers.keySet()) {
				if(System.currentTimeMillis()-lastReceived.get(p)<30000) {
					out.print("<tr>");
					out.println("<td>"+workers.get(p).port+"</td>");
					out.println("<td>"+workers.get(p).status+"</td>");
					out.println("<td>"+workers.get(p).job+"</td>");
					out.println("<td>"+workers.get(p).keysRead+"</td>");
					out.println("<td>"+workers.get(p).keysWritten+"</td>");
					out.print("</tr>");
				}
				else {
					workers.remove(p);
				}
		}
		out.println("</table>");
		out.println("</br></br>");
		out.println("<h3>Form:</h3>");
		out.println("<form name =\"statusform\" action = \"/sendRunMap\" method = \"POST\"><br/><br/>");
		out.println("ClassName of Job:&nbsp;&nbsp;&nbsp;&nbsp;<input type = \"text\" value = \"edu.upenn.cis455.mapreduce.job.WordCount\" name = \"job\"/><br/><br/>");
		out.println("Input Directory:&nbsp;&nbsp;&nbsp;&nbsp;<input type = \"text\" value = \"input\" name = \"inputDir\"/><br/><br/>");
		out.println("Output Directory:&nbsp;&nbsp;&nbsp;<input type = \"text\"value = \"output\" name = \"outputDir\"/><br/><br/>");
		out.println("Number of map threads:&nbsp;&nbsp;<input type = \"text\" value = \"8\" name = \"numOfMapThreads\"/><br/><br/>");
		out.println("Number of reduce threads:&nbsp;&nbsp;&nbsp;<input type = \"text\" value = \"5\" name = \"numOfReduceThreads\"/><br/><br/>");
		out.println("<input type = \"submit\" value = \"Submit\"/>");
		out.println("</form>");
		out.println("</body></html>");
		out.flush();
		out.close();

	}

	public void handleWorkerStatus(HttpServletRequest request, HttpServletResponse response) throws IOException {
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");

		String IPAddress = (String)request.getAttribute("IPAddress");

		WorkerStatus ws = new WorkerStatus();
		ws.port = Integer.parseInt(request.getParameter("port"));;
		ws.job = request.getParameter("job");
		ws.status = request.getParameter("status");
		ws.keysRead = Integer.parseInt(request.getParameter("keysRead"));
		ws.keysWritten = Integer.parseInt(request.getParameter("keysWritten"));

		workers.put(ws.port, ws);
		Long time = System.currentTimeMillis();
		lastReceived.put(ws.port, time);
		lastReceivedWithIP.put(IPAddress+":"+ws.port, time);
		out.close();
	}

	private void handleSendReduce() {

		//System.out.println("Inside send reduce");
		/*
		runMapParams.put("job",request.getParameter("job"));
		runMapParams.put("input",request.getParameter("inputDir"));
		runMapParams.put("numOfMapThreads",request.getParameter("numOfMapThreads"));
		runMapParams.put("numWorkers",Integer.toString(workers.size()));
		runMapParams.put("output", request.getParameter("outputDir"));
		runMapParams.put("numOfReduceThreads",request.getParameter("numOfReduceThreads"));
		 */

		for(int p: workers.keySet()) {
			String url = "http://"+"localhost"+":"+workers.get(p).port+"/runreduce";

			//System.out.println(url);
			String urlParameters = "";
			urlParameters += "job="+runMapParams.get("job")+"&";
			urlParameters += "output="+runMapParams.get("output")+"&";
			urlParameters += "numThreads="+runMapParams.get("numOfReduceThreads");
			//	System.out.println("number of reduce threads:"+runMapParams.get("numOfReduceThreads"));

			MasterThreads m = new MasterThreads(url, urlParameters);
			m.start();

		}
	}


	private boolean areAllWaiting() {
		//System.out.println("Checking if all are waiting");
		for(int w: workers.keySet()) {
			if(!workers.get(w).status.equals("waiting"))
				return false;
		}
		return true;
	}

	private void handleSendRunmap(HttpServletRequest request, HttpServletResponse response) throws IOException {
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");

		out.println("<html><body>Map jobs have been sent to workers<br/><br/><br/>");
		out.println("<form name =\"statusform\" action = \"/status\" method = \"GET\"><br/><br/>");
		out.println("<input type = \"submit\" value = \"Go to Status Page\"/>");
		out.println("</body></html>");

		//String IPAddress = (String)request.getAttribute("IPAddress");
		//	System.out.println("Inside runmap");
		runMapParams.put("job",request.getParameter("job"));
		runMapParams.put("input",request.getParameter("inputDir"));
		runMapParams.put("numOfMapThreads",request.getParameter("numOfMapThreads"));
		runMapParams.put("numWorkers",Integer.toString(workers.size()));
		runMapParams.put("output", request.getParameter("outputDir"));
		runMapParams.put("numOfReduceThreads",request.getParameter("numOfReduceThreads"));

		for(int p: workers.keySet()) {

			String url = "http://"+"localhost"+":"+workers.get(p).port+"/runmap";

			System.out.println(url);
			String urlParameters = "";
			urlParameters += "job="+runMapParams.get("job")+"&";
			urlParameters += "input="+runMapParams.get("input")+"&";
			urlParameters += "numThreads="+runMapParams.get("numOfMapThreads")+"&";
			urlParameters += "numWorkers="+runMapParams.get("numWorkers")+"&";
			int i = 0;
			for(String w: lastReceivedWithIP.keySet()) {
				urlParameters += "worker"+i+"="+w+"&";
				i++;
			}
			urlParameters = urlParameters.substring(0,urlParameters.length()-1);

			MasterThreads m = new MasterThreads(url, urlParameters);
			m.start();
		}

		System.out.println("lets check if all are waiting");

		while(!areAllWaiting())
		{
			//System.out.println("not all waiting");
		}

		handleSendReduce();

	}
}




