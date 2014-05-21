package edu.upenn.cis455.mapreduce.job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet.WorkerStatus;

public class MyContext implements Context {

	int numOfWorkers;
	HashMap<Integer, BigInteger> ranges;
	String storageDir, output;
	WorkerStatus ws;

	public MyContext(int n, String storage, int myId, WorkerStatus w) {
		generateRanges(n);
		storageDir = storage;
		createSpoolDirs(storage, myId);
		ws = w;
		
	}

	public MyContext(String str, WorkerStatus w, String o) {
		storageDir = str;
		ws = w;
		output = o;
		System.out.println("output:"+output);
		File fOutput = new File(storageDir+output);
		if(fOutput.exists()) {
			System.out.println("Deleting output Dir");
			deleteDirectory(fOutput);
			fOutput.delete();
		}
		if(new File(storageDir+output).mkdir()) {
			System.out.println("Output Directory has been created");
		}

	}

	private void generateRanges(int n) {
		numOfWorkers = n;
		String min = "0000000000000000000000000000000000000000";
		String max = "ffffffffffffffffffffffffffffffffffffffff";
		BigInteger maxInt = new BigInteger(max, 16);
		BigInteger minInt = new BigInteger(min, 16);
		BigInteger num = new BigInteger(Integer.toString(numOfWorkers));

		BigInteger range = maxInt.divide(num);

		ranges = new HashMap<Integer, BigInteger>();
		for(int i = 0; i<numOfWorkers; i++) {
			ranges.put(i, minInt.add(range));
			minInt = minInt.add(range);
		}
	}

	private String getHash(String key) throws NoSuchAlgorithmException {
		MessageDigest md;
		md = MessageDigest.getInstance("SHA-1");
		md.update(key.getBytes());

		byte byteData[] = md.digest();


		StringBuffer sb = new StringBuffer();
		for (int j = 0; j < byteData.length; j++) {
			sb.append(Integer.toString((byteData[j] & 0xff) + 0x100, 16).substring(1));
		}

		//	System.out.println("Hex format : " + sb.toString());
		return sb.toString();

	}


	private void createSpoolDirs(String storage, int myId) {
		try{
			File fSpoolOut = new File(storage+"spoolout");
			if(fSpoolOut.exists()) {
				System.out.println("Deleting Spool Out Dir");
				deleteDirectory(fSpoolOut);
				fSpoolOut.delete();
			}

			if(new File(storage+"spoolout").mkdir()) {
				System.out.println("Spool Out Directory has been created");
			}

			for(int i = 0; i<numOfWorkers; i++) {
				File f = new File(storage+"spoolout/worker"+i+".txt");
				if(f.createNewFile())
					System.out.println("Spoolout files created");

			}

			File fSpoolIn = new File(storage+"spoolin");
			if(fSpoolIn.exists()) {
				System.out.println("Deleting Spool In Dir");
				deleteDirectory(fSpoolIn);
				fSpoolIn.delete();
			}


			if(new File(storage+"spoolin").mkdir()) {
				System.out.println("Spool In Directory has been created");
			}

			//	File f = new File(storage+"spoolin/worker"+myId+".txt");
			File f = new File(storage+"spoolin/toreduce.txt");
			if(f.createNewFile())
				System.out.println("Spoolin files created");

		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}

	private void deleteDirectory(File dir) {
		for(File f: dir.listFiles()) {
			if(f.isDirectory())
				deleteDirectory(f);
			else 
				f.delete();
		}
	}

	@Override
	public void write(String key, String value) {
		if(key==null||value==null)
			return;
		if(key.equals("null")||value.equals("null"))
			return;
		String fileName;
		if(ws.status.equals("mapping")) {
			int id = determineWorker(key);
			fileName = storageDir+"spoolout/worker"+id+".txt";
		}
		else
			fileName = storageDir+output+"/result.txt";
		FileWriter fw;
		try {
			fw = new FileWriter(fileName,true);
			fw.write(key+"\t"+value+"\n");
			fw.close();
			ws.keysWritten++;	
		} catch (IOException e) {
			e.printStackTrace();
		} 

	}

	public void writeToSpoolIn(int id, String content) {
		String fileName = storageDir+"spoolin/toreduce.txt";
		FileWriter fw;
		try {
			fw = new FileWriter(fileName,true);
			fw.write(content+"\n");
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 

	}

	private int determineWorker(String key) {
		try {
			String hash = getHash(key);
			BigInteger hashInt = new BigInteger(hash, 16);

			for(int i = 0; i<numOfWorkers; i++) {
				if((hashInt.compareTo(ranges.get(i)))<0) {
			//		System.out.println(key+" is assigned to worker"+i);
					return i;
				}

			}

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public HashMap<String, String> getPushData() {
		HashMap<String, String> workerPushData = new HashMap<String, String>();
		for(int i = 0; i<numOfWorkers; i++) {
			String fileName = storageDir+"spoolout/worker"+i+".txt";
			String fileContent = "";
			try {
				BufferedReader br = new BufferedReader(new FileReader(fileName));
				String line;
				while((line = br.readLine())!=null) {
					if(line.equals("")||line.equals("\n"))
						continue;
					fileContent += line+"\n";
				}
				br.close();
				
			//	System.out.println("Pushing: worker"+i);
			//	System.out.println(fileContent);
				workerPushData.put("worker"+i, fileContent);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return workerPushData;
	}

	public void sortToReduce() {
		System.out.println("sorting file");
		String fileName = storageDir+"spoolin/toreduce.txt";
		try {
			//String cmd[] = {" sort ", fileName, " > ", storageDir+"spoolin/reduce.txt"};
			Process p = Runtime.getRuntime().exec("sort "+fileName);
			BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));

			File f = new File(storageDir+"spoolin/reduce.txt");

			FileWriter fw = new FileWriter(f.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			String line;
			while((line=r.readLine())!=null)
				bw.write(line+"\n");
			bw.close();
			r.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
