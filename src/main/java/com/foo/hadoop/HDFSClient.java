package com.foo.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HDFSClient {
  public Configuration conf = null;

  public HDFSClient() {
    conf = new Configuration();
    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));
    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/mapred-site.xml"));
  }

  public static void printUsage(){
    System.out.println("Usage: hdfsclient add" + "<local_path> <hdfs_path>");
    System.out.println("Usage: hdfsclient read" + "<hdfs_path>");
    System.out.println("Usage: hdfsclient delete" + "<hdfs_path>");
    System.out.println("Usage: hdfsclient mkdir" + "<hdfs_path>");
    System.out.println("Usage: hdfsclient copyfromlocal" + "<local_path> <hdfs_path>");
    System.out.println("Usage: hdfsclient copytolocal" + " <hdfs_path> <local_path> ");
    System.out.println("Usage: hdfsclient modificationtime" + "<hdfs_path>");
    System.out.println("Usage: hdfsclient getblocklocations" + "<hdfs_path>");
    System.out.println("Usage: hdfsclient gethostnames");
  }

  public boolean ifExists (Path source) throws IOException{

    Configuration config = new Configuration();
    config.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    config.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    config.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem hdfs = FileSystem.get(config);
    boolean isExists = hdfs.exists(source);
    return isExists;
  }

  public void getHostnames () throws IOException{
    Configuration config = new Configuration();
    config.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    config.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    config.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem fs = FileSystem.get(config);
    DistributedFileSystem hdfs = (DistributedFileSystem) fs;
    DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

    String[] names = new String[dataNodeStats.length];
    for (int i = 0; i < dataNodeStats.length; i++) {
      names[i] = dataNodeStats[i].getHostName();
      System.out.println((dataNodeStats[i].getHostName()));
    }
  }

  public void getBlockLocations(String source) throws IOException{

    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem fileSystem = FileSystem.get(conf);
    Path srcPath = new Path(source);

// Check if the file already exists
    if (!(ifExists(srcPath))) {
      System.out.println("No such destination " + srcPath);
      return;
    }
// Get the filename out of the file path
    String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

    FileStatus fileStatus = fileSystem.getFileStatus(srcPath);

    BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    int blkCount = blkLocations.length;

    System.out.println("File :" + filename + "stored at:");
    for (int i=0; i < blkCount; i++) {
      String[] hosts = blkLocations[i].getHosts();
      System.out.format("Host %d: %s %n", i, hosts);
    }

  }

  public void getModificationTime(String source) throws IOException{

    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem fileSystem = FileSystem.get(conf);
    Path srcPath = new Path(source);

// Check if the file already exists
    if (!(fileSystem.exists(srcPath))) {
      System.out.println("No such destination " + srcPath);
      return;
    }
// Get the filename out of the file path
    String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

    FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
    long modificationTime = fileStatus.getModificationTime();

    System.out.format("File %s; Modification time : %0.2f %n",filename,modificationTime);

  }

  public void copyFromLocal (String source, String dest) throws IOException {

    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem fileSystem = FileSystem.get(conf);
    Path srcPath = new Path(source);

    Path dstPath = new Path(dest);
// Check if the file already exists
    if (!(fileSystem.exists(dstPath))) {
      System.out.println("No such destination " + dstPath);
      return;
    }

// Get the filename out of the file path
    String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

    try{
      fileSystem.copyFromLocalFile(srcPath, dstPath);
      System.out.println("File " + filename + "copied to " + dest);
    }catch(Exception e){
      System.err.println("Exception caught! :" + e);
      System.exit(1);
    }finally{
      fileSystem.close();
    }
  }

  public void copyToLocal (String source, String dest) throws IOException {

    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem fileSystem = FileSystem.get(conf);
    Path srcPath = new Path(source);

    Path dstPath = new Path(dest);
// Check if the file already exists
    if (!(fileSystem.exists(srcPath))) {
      System.out.println("No such destination " + srcPath);
      return;
    }

// Get the filename out of the file path
    String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

    try{
      fileSystem.copyToLocalFile(srcPath, dstPath);
      System.out.println("File " + filename + "copied to " + dest);
    }catch(Exception e){
      System.err.println("Exception caught! :" + e);
      System.exit(1);
    }finally{
      fileSystem.close();
    }
  }

  public void renameFile (String fromthis, String tothis) throws IOException{
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem fileSystem = FileSystem.get(conf);
    Path fromPath = new Path(fromthis);
    Path toPath = new Path(tothis);

    if (!(fileSystem.exists(fromPath))) {
      System.out.println("No such destination " + fromPath);
      return;
    }

    if (fileSystem.exists(toPath)) {
      System.out.println("Already exists! " + toPath);
      return;
    }

    try{
      boolean isRenamed = fileSystem.rename(fromPath, toPath);
      if(isRenamed){
        System.out.println("Renamed from " + fromthis + "to " + tothis);
      }
    }catch(Exception e){
      System.out.println("Exception :" + e);
      System.exit(1);
    }finally{
      fileSystem.close();
    }

  }

  public void addFile(String source, String dest) throws IOException {

// Conf object will read the HDFS configuration parameters
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem fileSystem = FileSystem.get(conf);

// Get the filename out of the file path
    String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

// Create the destination path including the filename.
    if (dest.charAt(dest.length() - 1) != '/') {
      dest = dest + "/" + filename;
    } else {
      dest = dest + filename;
    }

// Check if the file already exists
    Path path = new Path(dest);
    if (fileSystem.exists(path)) {
      System.out.println("File " + dest + " already exists");
      return;
    }

// Create a new file and write data to it.
    FSDataOutputStream out = fileSystem.create(path);
    InputStream in = new BufferedInputStream(new FileInputStream(
        new File(source)));

    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
      out.write(b, 0, numBytes);
    }

// Close all the file descripters
    in.close();
    out.close();
    fileSystem.close();
  }

  public void readFile(String file) throws IOException {

    FileSystem fileSystem = FileSystem.get(this.conf);

    Path path = new Path(file);
    if (!fileSystem.exists(path)) {
      System.out.println("File " + file + " does not exists");
      return;
    }

    FSDataInputStream in = fileSystem.open(path);

    String filename = file.substring(file.lastIndexOf('/') + 1,
        file.length());

    OutputStream out = new BufferedOutputStream(new FileOutputStream(
        new File(filename)));

    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
      out.write(b, 0, numBytes);
    }

    in.close();
    out.close();
    fileSystem.close();
  }

  public void deleteFile(String file) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));

    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(file);
    if (!fileSystem.exists(path)) {
      System.out.println("File " + file + " does not exists");
      return;
    }

    fileSystem.delete(new Path(file), true);

    fileSystem.close();
  }

  public void mkdir(String dir) throws IOException {

    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(dir);
    if (fileSystem.exists(path)) {
      System.out.println("Dir " + dir + " already exists!");
      return;
    }

    fileSystem.mkdirs(path);

    fileSystem.close();
  }
}
