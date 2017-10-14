import java.io.IOException;
import mpi.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Level;
public class OperatingFiles {
	//initialization
	int myrank;
	int npros;
	int fileNum;
	int dirNum;
	private static final int treeDepth = 8;
	private static final int treeValue = 3;
	int opTime = 1000;
	int bufferSize = 1024 * 1024;
	OperatingFiles(int _myrank, int _npros, int _opTime, int _bufferSize) {
		this.myrank = _myrank;
		this.npros = _npros;
		this.fileNum = 0;
		this.dirNum = 0;
		this.opTime = _opTime;
		this.bufferSize = _bufferSize;
	}
	static Configuration conf = new Configuration();
	static FileSystem hdfs;
	static {
		LogManager.getRootLogger().setLevel(Level.OFF);
		String path = "/home/chenyoumin/rdma-hadoop-2.x-1.1.0/etc/hadoop/";
		conf.addResource(new Path(path + "core-site.xml"));
		conf.addResource(new Path(path + "hdfs-site.xml"));
		conf.addResource(new Path(path + "mapred-site.xml"));
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//create a direction
	public void createDir(String dir) throws IOException {
		Path path = new Path(dir);
		hdfs.mkdirs(path);
	}	
	public void createMultipleDirs(String path, int depth) throws IOException {
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
			}
		}
	}
	//copy from local file to HDFS file
	public void copyFile(String localSrc, String hdfsDst) throws IOException{
		Path src = new Path(localSrc);		
		Path dst = new Path(hdfsDst);
		hdfs.copyFromLocalFile(src, dst);
	}
	
	//create a new file
	public void createFile(String fileName) throws IOException {
		Path dst = new Path(fileName);
		FSDataOutputStream output = hdfs.create(dst);
		output.close();
	}

	public void createMultipleFiles(String path, int depth) throws IOException {
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
			}
		}
	}

	//list all files
	public void listFiles(String dirName) throws IOException {
		Path f = new Path(dirName);
		hdfs.listStatus(f);
	}

	public void statMultipleDirectories(String path, int depth) throws IOException {
		if (depth < treeDepth) {
			for (int i = 0; i < treeValue; i++) {
				if (depth == 0) {
					path = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				} else {
					path = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				}
				listFiles(path);
				statMultipleDirectories(path, depth + 1);
			}
		}
	}

	//judge existence.
	public void fileStat(String fileName) throws IOException {
		Path f = new Path(fileName);
		hdfs.exists(f);
	}

	public void statMultipleFiles(String path, int depth) throws IOException {
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
			}
		}
	}

	//judge a file existed? and delete it!
	public void deleteFile(String fileName) throws IOException {
		Path f = new Path(fileName);
		hdfs.delete(f,true);
	}

	public void removeMultipleFiles(String path, int depth) throws IOException {
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
			}
		}
	}

	public void removeMultipleDirectories(String path, int depth) throws IOException {
		if (depth < treeDepth) {
			for (int i = 0; i < treeValue; i++) {
				if (depth == 0) {
					path = "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				} else {
					path = path + "/dir_" + Integer.toString(myrank) + "." + Integer.toString(i);
				}
				statMultipleDirectories(path, depth + 1);
				deleteFile(path);
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
			if (num == 0) {
				System.out.println("-------time = " + maxTimeCost);
				return;
			}
			long rate = num * npros * 1000 / maxTimeCost;
			System.out.println("opnum = " + (num * npros) + " time = " + maxTimeCost + " rate = " + rate);
		}
	}

	public long readFile(String fileName) throws IOException {
		long sysDate1, sysDate2;
		byte[] bytes = new byte[bufferSize];
		Path path = new Path(fileName);
		//int len;
		FSDataInputStream in = hdfs.open(path);

		sysDate1 = System.currentTimeMillis();
		for (int i = 0; i < opTime; i++) {
			//System.out.println("begin read");
			in.read(bytes);
			//System.out.println("len = " + len);
		}
		sysDate2 = System.currentTimeMillis();
		//long rate = opTime * bufferSize * 1000 / (sysDate2 - sysDate1);
		// System.out.println("read time = " + (sysDate2 - sysDate1));
		in.close();
		return (sysDate2 - sysDate1);
	}

	public long writeFile(String fileName) throws IOException {
		long sysDate1, sysDate2;
		byte[] bytes = new byte[bufferSize];
		Path dst = new Path(fileName);
		FSDataOutputStream output = hdfs.create(dst);

		sysDate1 = System.currentTimeMillis();
		for (int i = 0; i < opTime; i++) {
			output.write(bytes);
			// output.hsync();
		}
		sysDate2 = System.currentTimeMillis();

		//long rate = opTime * bufferSize / (sysDate2 - sysDate1);
		// System.out.println("write time = " + (sysDate2 - sysDate1));
		output.close();
		return (sysDate2 - sysDate1);
	}
	public static void main(String[] args) throws IOException, MPIException {
		MPI.Init(args);
		int myrank = MPI.COMM_WORLD.getRank();
        int size = MPI.COMM_WORLD.getSize() ;
        long sysDate1, sysDate2, rate, temp;
        String path = new String();
		OperatingFiles ofs = new OperatingFiles(myrank, size, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
		// temp = ofs.writeFile("/file" + Integer.toString(myrank));
		// ofs.timeCounter(temp, false);
		MPI.COMM_WORLD.barrier();
		temp = ofs.readFile("/file" + Integer.toString(myrank));
		ofs.timeCounter(temp, false);
		/*
		path = "";
		sysDate1 = System.currentTimeMillis();
		ofs.createMultipleDirs(path, 0);
		sysDate2 = System.currentTimeMillis();
		ofs.timeCounter(sysDate2 - sysDate1, false);

		path = "";
		sysDate1 = System.currentTimeMillis();
		ofs.createMultipleFiles(path, 0);
		sysDate2 = System.currentTimeMillis();
		ofs.timeCounter(sysDate2 - sysDate1, true);

		path = "";
		sysDate1 = System.currentTimeMillis();
		ofs.statMultipleFiles(path, 0);
		sysDate2 = System.currentTimeMillis();
		ofs.timeCounter(sysDate2 - sysDate1, true);

		path = "";
		sysDate1 = System.currentTimeMillis();
		ofs.statMultipleDirectories(path, 0);
		sysDate2 = System.currentTimeMillis();
		ofs.timeCounter(sysDate2 - sysDate1, false);

		path = "";
		sysDate1 = System.currentTimeMillis();
		ofs.removeMultipleFiles(path, 0);
		sysDate2 = System.currentTimeMillis();
		ofs.timeCounter(sysDate2 - sysDate1, true);

		path = "";
		sysDate1 = System.currentTimeMillis();
		ofs.removeMultipleDirectories(path, 0);
		sysDate2 = System.currentTimeMillis();
		ofs.timeCounter(sysDate2 - sysDate1, false);
		*/
		MPI.Finalize();
		return;
	}
}
