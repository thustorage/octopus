import java.io.IOException;
import java.io.Writer; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Level;
public class NVFSBandwidth {
	//initialization
	int fileNum;
	int dirNum;
	byte[] bytes;
	long sysDate1, sysDate2;
	private static final int bufferSize = 1024 * 1024;
	private static final int opTime = 1000;
	NVFSBandwidth() {
		this.fileNum = 0;
		this.dirNum = 0;
		bytes = new byte[bufferSize];
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

	public void readFile(String fileName) throws IOException {
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
		long rate = opTime * bufferSize * 1000 / (sysDate2 - sysDate1);
		System.out.println("read time = " + (sysDate2 - sysDate1));
		in.close();
	}

	public void writeFile(String fileName) throws IOException {
		Path dst = new Path(fileName);
		FSDataOutputStream output = hdfs.create(dst);

		sysDate1 = System.currentTimeMillis();
		for (int i = 0; i < opTime; i++) {
			output.write(bytes);
			// output.hsync();
		}
		sysDate2 = System.currentTimeMillis();

		long rate = opTime * bufferSize / (sysDate2 - sysDate1);
		System.out.println("write rate = " + rate);
		output.close();
	}

	//judge a file existed? and delete it!
	public void deleteFile(String fileName) throws IOException {
		Path f = new Path(fileName);
		hdfs.delete(f,true);
	}

	public static void main(String[] args) throws IOException {
        long sysDate1, sysDate2, rate;
        String path = new String();
		NVFSBandwidth ofs = new NVFSBandwidth();
		path = "/opfile";
		ofs.writeFile(path);
		ofs.readFile("/readfile");
		ofs.deleteFile(path);
		return;
	}
}
