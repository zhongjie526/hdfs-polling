package com.westpac.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

@Slf4j
@Configuration
public class HDFSConfiguration {
  
  @Value("${namenode}") String hdfsServer;
  @Value("${kerberos.keytab}") String keytab;
  @Value("${kerberos.user}") String kerberosUser;
  @Value("${kerberos.realm}") String realm;
  @Value("${kerberos.kdc}") String kdc;
  @Value("${kerberos.principal.pattern}") String principalPattern;

  @Bean
  public org.apache.hadoop.conf.Configuration config() throws IOException {
//    System.setProperty("HADOOP_USER_NAME", hdfsUser);
    System.setProperty("java.security.krb5.realm",realm);
    System.setProperty("java.security.krb5.kdc",kdc);

    org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration(true);
    log.info("Connecting to "+hdfsServer);
    config.set("fs.defaultFS", hdfsServer);
//    config.set("fs.defaultFS", "hdfs://zzhang-2.vpc.cloudera.com:8020/");
    config.set("hadoop.security.authentication", "kerberos");
    config.set("hadoop.security.authorization", "true");
    config.set("dfs.namenode.kerberos.principal.pattern", principalPattern);

    UserGroupInformation.setConfiguration(config);
    UserGroupInformation.loginUserFromKeytab(kerberosUser,keytab);

    return config;
  }

  @Bean
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(config());
  }
  
}
