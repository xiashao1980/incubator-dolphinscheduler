/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.common.utils;

import org.apache.commons.lang3.SystemUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ResUploadType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * common utils
 */
public class  CommonUtils {

  private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

  /**
   * @return get the path of system environment variables
   */
  public static String getSystemEnvPath() {
    String envPath = PropertyUtils.getString(Constants.DOLPHINSCHEDULER_ENV_PATH);
    if (StringUtils.isEmpty(envPath)) {
      envPath = System.getProperty("user.home") + File.separator + ".bash_profile";
    }

    if(OSUtils.isWindows())
    {
      //如果是windows, 则使用jar包所在路径的上面一层的
      String jarPath = CommonUtils.getCurrentJarPath();
      String newPath = (jarPath + envPath).replace('/','\\');
      return newPath;
    }

    return envPath;
  }

  public static String getCurrentJarPath(){

    String filePath = System.getProperty("java.class.path");
    String pathSplit = System.getProperty("path.separator");//windows下是";",linux下是":"

    String markClassName = "\\classes";

    String[] filePathes = filePath.split(pathSplit);
    for(int i=0;i<filePathes.length;i++){
      String curPath = filePathes[i];

      if(curPath.length() < markClassName.length())
        continue;

      String tail = curPath.substring(curPath.length() - markClassName.length());
      if(tail.equals(markClassName)){
        return curPath;
      }

    }

    if(filePath.contains(pathSplit)){
      filePath = filePath.substring(0,filePath.indexOf(pathSplit));
    }else if (filePath.endsWith(".jar")) {//截取路径中的jar包名,可执行jar包运行的结果里包含".jar"

      //此时的路径是"E:\workspace\Demorun\Demorun_fat.jar"，用"/"分割不行
      //下面的语句输出是-1，应该改为lastIndexOf("\\")或者lastIndexOf(File.separator)
//			System.out.println("getPath2:"+filePath.lastIndexOf("/"));
      filePath = filePath.substring(0, filePath.lastIndexOf(File.separator) + 1);

    }
    return filePath;




  }

  /**
   * @return get queue implementation name
   */
  public static String getQueueImplValue(){
    return PropertyUtils.getString(Constants.SCHEDULER_QUEUE_IMPL);
  }

  /**
   * 
   * @return is develop mode
   */
  public static boolean isDevelopMode() {
    return PropertyUtils.getBoolean(Constants.DEVELOPMENT_STATE);
  }



  /**
   * if upload resource is HDFS and kerberos startup is true , else false
   * @return true if upload resource is HDFS and kerberos startup
   */
  public static boolean getKerberosStartupState(){
    String resUploadStartupType = PropertyUtils.getString(Constants.RES_UPLOAD_STARTUP_TYPE);
    ResUploadType resUploadType = ResUploadType.valueOf(resUploadStartupType);
    Boolean kerberosStartupState = PropertyUtils.getBoolean(Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE);
    return resUploadType == ResUploadType.HDFS && kerberosStartupState;
  }

  /**
   * load kerberos configuration
   * @throws Exception errors
   */
  public static void loadKerberosConf()throws Exception{
    if (CommonUtils.getKerberosStartupState())  {
      System.setProperty(Constants.JAVA_SECURITY_KRB5_CONF, PropertyUtils.getString(Constants.JAVA_SECURITY_KRB5_CONF_PATH));
      Configuration configuration = new Configuration();
      configuration.set(Constants.HADOOP_SECURITY_AUTHENTICATION, Constants.KERBEROS);
      UserGroupInformation.setConfiguration(configuration);
      UserGroupInformation.loginUserFromKeytab(PropertyUtils.getString(Constants.LOGIN_USER_KEY_TAB_USERNAME),
              PropertyUtils.getString(Constants.LOGIN_USER_KEY_TAB_PATH));
    }
  }
}
