package com.westpac;

import com.westpac.services.HDFSFileService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

@Slf4j
@SpringBootApplication
public class HDFSFileApplication implements CommandLineRunner {

	private final HDFSFileService HDFSFileService;

	@Value("${directories}")
	private String[] folders;

	public HDFSFileApplication(HDFSFileService HDFSFileService) {
		this.HDFSFileService = HDFSFileService;
	}

	public static void main(String[] args) {
		SpringApplication.run(HDFSFileApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("Starting DDEP HDFS Polling");
		log.info("******************************************************************************************");
		log.info(folders.length + " folder(s) to poll");

		for (String folder : folders) {
			HDFSFileService.listFolder(folder);
		}

		log.info("******************************************************************************************");
		log.info("Finished DDEP HDFS Polling");
	}
}
