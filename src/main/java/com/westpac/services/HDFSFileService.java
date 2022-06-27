package com.westpac.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Date;

@Slf4j
@Service
public class HDFSFileService {
  
  private final FileSystem hdfs;
  
  @Autowired
  public HDFSFileService(FileSystem hdfs){
    this.hdfs = hdfs;
  }
  
  public void createFolder(String folderName) throws IOException{
    log.info("Home folder:"+hdfs.getHomeDirectory().toString() );
    Path folderPath = new Path(hdfs.getHomeDirectory().toString() + "/" + folderName);
  
    if (!hdfs.exists(folderPath)) {
      hdfs.mkdirs(folderPath);
      log.info("Folder created: "+folderPath);
//      hdfs.close();
    }else{
//      hdfs.close();
      log.info("The folder " + folderPath.toUri().getPath() + " already exists");
    }
  }
  
  public void createFile(String folderName, String fileName) throws IOException {
    Path folderPath = new Path(hdfs.getHomeDirectory().toString() + "/" + folderName);
  
    if (hdfs.exists(folderPath)) {
      Path filePath = new Path(folderPath + "/" + fileName);
      if (!hdfs.exists(filePath)){
        hdfs.create(filePath);
      }else{
        log.info("The file " + fileName + " already exists in folder " + folderPath.toUri().getPath());
      }
    }else{
      log.error("The folder " + folderPath.toUri().getPath() + " does not exist");
    }
//    hdfs.close();
  }

  public void readFile(String folderName, String fileName)throws IOException {
    Path filepath = new Path(folderName + "/" + fileName);

    FSDataInputStream inputStream = hdfs.open(filepath);
    System.out.println(inputStream.available());

  }

  public void listFile(String folderName, String fileName) throws IOException {

    Path filepath = new Path(folderName + "/" + fileName);

    FileStatus[] status = hdfs.listStatus(filepath);

    for (FileStatus fileStatus : status) {
      long lastAccessTimeLong = fileStatus.getAccessTime();
      Date lastAccessTimeDate = new Date(lastAccessTimeLong);

      log.info("Last access time for file: "+filepath+": "+lastAccessTimeDate);
    }
  }

  public void listFolder(String folderName) throws IOException {
    log.info("Listing "+folderName);
      FileStatus[] status = hdfs.listStatus(new Path(folderName));

      long lastAccessTimeLong = 0;
      long folderSize = 0;

      for (FileStatus fileStatus : status) {
        if (fileStatus.getAccessTime() > lastAccessTimeLong) {
          lastAccessTimeLong=fileStatus.getAccessTime();
        }

        long fileLength = fileStatus.getLen();

        folderSize += fileLength;
      }
      Date lastAccessTimeDate = new Date(lastAccessTimeLong);

    log.info("=========================================================================================");
      log.info("Directory: "+folderName);
      log.info("Last access time: "+lastAccessTimeDate);
      log.info("Data size: "+folderSize*10/1024/10.0+"K");
    }

}
