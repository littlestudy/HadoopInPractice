package test;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PrintFileInfo {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String path = "hdfs://masternh:9000/data/1/num1";
		Path filePath = new Path(path);
		FileSystem fs = filePath.getFileSystem(conf);
		FileStatus fileStatus = fs.getFileStatus(filePath);
		
        System.out.println("文件路径: "+fileStatus.getPath()); 
        System.out.println("文件长度: "+fileStatus.getLen()); 
        System.out.println("文件修改日期： "+new Timestamp (fileStatus.getModificationTime()).toString()); 
        System.out.println("文件上次访问日期： "+new Timestamp(fileStatus.getAccessTime()).toString()); 
        System.out.println("文件备份数： "+fileStatus.getReplication()); 
        System.out.println("文件的块大小： "+fileStatus.getBlockSize()); 
        System.out.println("文件所有者：  "+fileStatus.getOwner()); 
        System.out.println("文件所在的分组： "+fileStatus.getGroup()); 
        System.out.println("文件的 权限： "+fileStatus.getPermission().toString()); 
        
        BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation bl : blkLocations)
        	System.out.println(bl);
        
        System.out.println("-------------------------");
        
        long blockSize = fileStatus.getBlockSize();
        long splitSize = blockSize;
        long bytesRemaining = fileStatus.getLen();
        double SPLIT_SLOP = 1.1;   // 10% slop
        long length = bytesRemaining;
        System.out.println(bytesRemaining + " / " + splitSize);
        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
        	int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
        	// splits.add(new FileSplit(path, length-bytesRemaining, splitSize, blkLocations[blkIndex].getHosts()));
        	System.out.println(
        			  "文件路径：" + path
        			+ ", 分块起始位置：" + (length-bytesRemaining)
        			+ ", 分块长度：" +splitSize
        			+ ", 分块所在主机：" + blkLocations[blkIndex].toString());
        	bytesRemaining -= splitSize;
        }
        
        if (bytesRemaining != 0){
        	//splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, blkLocations[blkLocations.length-1].getHosts()));
        	        	System.out.println(
        			  "文件路径：" + path
        			+ ", 分块起始位置：" + (length - bytesRemaining)
        			+ ", 分块长度：" + bytesRemaining
        			+ ", 分块所在主机：" + blkLocations[blkLocations.length - 1].toString());
        }
	}
	
	public static int getBlockIndex(BlockLocation[] blkLocations, long offset) {
		  for (int i = 0 ; i < blkLocations.length; i++) {
			  // is the offset inside this block?
			  if ((blkLocations[i].getOffset() <= offset) && (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
				  return i;
			  }
		  }
		  return -1;
    }
}
