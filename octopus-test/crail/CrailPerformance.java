import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import mpi.*;
import com.ibm.crail.CrailBufferedInputStream;
import com.ibm.crail.CrailBufferedOutputStream;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailMultiStream;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.GetOpt;
public class CrailPerformance {
	CrailConfiguration conf;
    CrailFS fs;
    int myrank;
    int npros;
    int dirNum;
    int fileNum;
    int op;
    int iosize;
    private static final int treeDepth = 3;
	private static final int treeValue = 6;
    CrailPerformance(CrailConfiguration _conf, CrailFS _fs, int _op, int _iosize) throws IOException, Exception  {
    	myrank = MPI.COMM_WORLD.getRank();
        npros = MPI.COMM_WORLD.getSize();
        this.conf = _conf;
        this.fs =_fs;
        this.op = _op;
        this.iosize = _iosize;
    }
    public void createFile(String path) throws Exception {
    	// System.out.println(path);
        fs.createFile(path, 0, 0).get().syncDir();
    }
    public long writeFile(String path) throws Exception {
    	
    	long _capacity = op * iosize;
    	ByteBuffer buf = null;
    	buf = ByteBuffer.allocateDirect(iosize);

    	ConcurrentLinkedQueue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();
    	bufferQueue.add(buf);
    	warmup(fs, path, 1, bufferQueue);

    	CrailFile file = fs.createFile(path, 0, 0).get();
    	CrailOutputStream directStream = file.getDirectOutputStream(_capacity);

    	long start = System.currentTimeMillis();
    	for (int i = 0; i < op; i++) {
    		buf.clear();
    		directStream.write(buf).get();
    	}
    	long end = System.currentTimeMillis();
    	directStream.close();
    	return (end - start);
    }
    public long readFile(String path) throws Exception {

    	ByteBuffer buf = null;
    	buf = ByteBuffer.allocateDirect(iosize);
    	buf.clear();

    	ConcurrentLinkedQueue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();
    	bufferQueue.add(buf);
    	warmup(fs, path, 1, bufferQueue);

    	CrailFile file = fs.lookupNode(path).get().asFile();
    	CrailInputStream directStream = file.getDirectInputStream(file.getCapacity());

    	long start = System.currentTimeMillis();
    	for (int i = 0; i < op; i++) {
    		buf.clear();
    		directStream.read(buf).get();
    	}
    	long end = System.currentTimeMillis();
    	directStream.close();
    	return (end - start);
    }

    private void warmup(CrailFS fs, String filename, int operations, ConcurrentLinkedQueue<ByteBuffer> bufferList) throws Exception {
    	String warmupFilename = filename + ".warmup";
    	if (operations > 0) {
    		CrailFile warmupFile = fs.createFile(warmupFilename, 0, 0).get();
    		CrailBufferedOutputStream warmupStream = warmupFile.getBufferedOutputStream(0, true);
    		for (int i = 0; i < operations; i++) {
    			ByteBuffer buf = bufferList.poll();
    			buf.clear();
    			warmupStream.write(buf);
    			bufferList.add(buf);
    		}
    		warmupStream.flush();
    		warmupStream.close();
    		fs.delete(warmupFilename, false).get().syncDir();
    	}
    }
    public void createMultipleDirs(String path, int depth) throws IOException, Exception {
		if (depth < treeDepth) {
			for (int i = 0; i < treeValue; i++) {
				if (depth == 0) {
					path = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				} else {
					path = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				}
				createDir(path);
				dirNum += 1;
				createMultipleDirs(path, depth + 1);
				path = path.substring(0, path.length()-8);
			}
		}
	}

    public void createDir(String path) throws Exception {
    	// System.out.println(path);
    	fs.makeDirectory(path).get().syncDir();
    }

	public void createMultipleFiles(String path, int depth) throws IOException, Exception {
		String file = new String();
		if (depth < treeDepth) {
			for (int i = 0; i < treeValue; i++) {
				for (int j = 0; j < treeValue; j++) {
					if (depth == 0) {
						file = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
						file = file + "/file_" + Integer.toString(myrank) + "." + Integer.toString(j);
					} else {
						file = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
						file = file + "/file_" + Integer.toString(myrank) + "." + Integer.toString(j);
					}
					createFile(file);
					fileNum += 1;
				}
				if (depth == 0) {
					path = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				} else {
					path = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				}
				createMultipleFiles(path, depth + 1);
				path = path.substring(0, path.length()-8);
			}
		}
	}

    public void listFiles(String path) throws Exception {
    	fs.lookupNode(path).get();
    }

	public void statMultipleDirectories(String path, int depth) throws IOException, Exception {
		if (depth < treeDepth) {
			for (int i = 0; i < treeValue; i++) {
				if (depth == 0) {
					path = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				} else {
					path = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				}
				listFiles(path);
				statMultipleDirectories(path, depth + 1);
				path = path.substring(0, path.length()-8);
			}
		}
	}

    public void fileStat(String path) throws Exception {
    	fs.lookupNode(path).get();
    }

    public void statMultipleFiles(String path, int depth) throws IOException, Exception {
		String file = new String();
		if (depth < treeDepth) {
			for (int i = 0; i < treeValue; i++) {
				for (int j = 0; j < treeValue; j++) {
					if (depth == 0) {
						file = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
						file = file + "/file_" + Integer.toString(myrank) + "." + Integer.toString(j);
					} else {
						file = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
						file = file + "/file_" + Integer.toString(myrank) + "." + Integer.toString(j);
					}
					fileStat(file);
				}
				if (depth == 0) {
					path = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				} else {
					path = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				}
				statMultipleFiles(path, depth + 1);
				path = path.substring(0, path.length()-8);
			}
		}
	}

    public void deleteFile(String path) throws Exception {
    	// System.out.println(path);
		fs.delete(path, true).get().syncDir();
	}

	public void removeMultipleFiles(String path, int depth) throws IOException, Exception {
		String file = new String();
		if (depth < treeDepth) {
			for (int i = 0; i < treeValue; i++) {
				for (int j = 0; j < treeValue; j++) {
					if (depth == 0) {
						file = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
						file = file + "/file_" + Integer.toString(myrank) + "." + Integer.toString(j);
					} else {
						file = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
						file = file + "/file_" + Integer.toString(myrank) + "." + Integer.toString(j);
					}
					deleteFile(file);
				}
				if (depth == 0) {
					path = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				} else {
					path = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				}
				removeMultipleFiles(path, depth + 1);
				path = path.substring(0, path.length()-8);
			}
		}
	}

	public void removeMultipleDirectories(String path, int depth) throws IOException, Exception {
		if (depth < treeDepth) {
			for (int i = 0; i < treeValue; i++) {
				if (depth == 0) {
					path = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				} else {
					path = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				}
				statMultipleDirectories(path, depth + 1);
				deleteFile(path);
				path = path.substring(0, path.length()-8);
			}
		}
	}

	public long collectTime (long cost) throws IOException, MPIException {
		long maxCost = cost;
		long[] tempCost = new long[1];
		for (int i = 1; i < npros; i++) {
			MPI.COMM_WORLD.recv(tempCost, 1, MPI.LONG, i, 99) ;
			if (tempCost[0] > maxCost) {
				maxCost = tempCost[0];
			}
		}
		return maxCost;
	}

	public void timeCounter(long cost, boolean isFile) throws IOException, MPIException {
		long maxTimeCost = 0;
		long[] mycost = new long[1];
		mycost[0] = cost;
		if (myrank != 0) {
			MPI.COMM_WORLD.send(mycost, 1, MPI.LONG, 0, 99) ;
		} else {
			maxTimeCost = collectTime(cost);
			long num = (isFile) ? fileNum : dirNum;
			if (num == 0 || maxTimeCost == 0) {
				System.out.println("time = " + maxTimeCost);
				return;
			}
			long rate = num * npros * 1000 / maxTimeCost;
			System.out.println("opnum = " + (num * npros) + " time = " + maxTimeCost + " rate = " + rate);
		}
	}

    public static void main(String[] args) throws IOException, Exception {
    MPI.Init(args);
    int myrank = MPI.COMM_WORLD.getRank();
    CrailConfiguration conf = new CrailConfiguration();
    CrailFS fs = CrailFS.newInstance(conf);
    CrailPerformance ofs = new CrailPerformance(conf, fs, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    ofs.createDir("/warmup" + Integer.toString(myrank));
    /*
    long ret = ofs.writeFile("/writeFile" + Integer.toString(myrank));
    ofs.timeCounter(ret, false);
    ret = ofs.readFile("/writeFile" + Integer.toString(myrank));
    ofs.timeCounter(ret, false);
    ofs.deleteFile("/writeFile" + Integer.toString(myrank));
    */
    long sysDate1, sysDate2, rate;
    String path = new String();
    
    System.out.println("mkdir");
	path = "";
	sysDate1 = System.currentTimeMillis();
	ofs.createMultipleDirs(path, 0);
	sysDate2 = System.currentTimeMillis();
	ofs.timeCounter(sysDate2 - sysDate1, false);

	 System.out.println("mknod");
	path = "";
	sysDate1 = System.currentTimeMillis();
	ofs.createMultipleFiles(path, 0);
	sysDate2 = System.currentTimeMillis();
	ofs.timeCounter(sysDate2 - sysDate1, true);

	 System.out.println("statfile");
	path = "";
	sysDate1 = System.currentTimeMillis();
	ofs.statMultipleFiles(path, 0);
	sysDate2 = System.currentTimeMillis();
	ofs.timeCounter(sysDate2 - sysDate1, true);

	System.out.println("statdir");
	path = "";
	sysDate1 = System.currentTimeMillis();
	ofs.statMultipleDirectories(path, 0);
	sysDate2 = System.currentTimeMillis();
	ofs.timeCounter(sysDate2 - sysDate1, false);

	System.out.println("rmnod");
	path = "";
	sysDate1 = System.currentTimeMillis();
	ofs.removeMultipleFiles(path, 0);
	sysDate2 = System.currentTimeMillis();
	ofs.timeCounter(sysDate2 - sysDate1, true);

	System.out.println("rmdir");
	path = "";
	sysDate1 = System.currentTimeMillis();
	ofs.removeMultipleDirectories(path, 0);
	sysDate2 = System.currentTimeMillis();
	ofs.timeCounter(sysDate2 - sysDate1, false);

	ofs.deleteFile("/warmup" + Integer.toString(myrank));
    fs.close();
    MPI.Finalize();
    return;
    }
}