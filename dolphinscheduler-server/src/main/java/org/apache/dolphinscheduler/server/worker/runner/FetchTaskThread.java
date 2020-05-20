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
package org.apache.dolphinscheduler.server.worker.runner;

import com.alibaba.fastjson.JSONObject;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.process.Property;
import org.apache.dolphinscheduler.common.queue.ITaskQueue;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.FileUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.common.zk.AbstractZKClient;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.dao.entity.AsyncCallbackMsg;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.dao.entity.WorkerGroup;
import org.apache.dolphinscheduler.server.zk.ZKWorkerClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *  fetch task thread
 */
public class FetchTaskThread implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(FetchTaskThread.class);
    /**
     *  set worker concurrent tasks
     */
    private final int taskNum;

    /**
     *  zkWorkerClient
     */
    private final ZKWorkerClient zkWorkerClient;

    /**
     * task queue impl
     */
    protected ITaskQueue taskQueue;

    /**
     *  process database access
     */
    private final ProcessDao processDao;

    /**
     *  worker thread pool executor
     */
    private final ExecutorService workerExecService;

    /**
     *  worker exec nums
     */
    private int workerExecNums;

    /**
     * conf
     */
    private Configuration conf;

    /**
     *  task instance
     */
    private TaskInstance taskInstance;

    /**
     * task instance id
     */
    Integer taskInstId;

    public FetchTaskThread(int taskNum, ZKWorkerClient zkWorkerClient,
                           ProcessDao processDao, Configuration conf,
                           ITaskQueue taskQueue){
        this.taskNum = taskNum;
        this.zkWorkerClient = zkWorkerClient;
        this.processDao = processDao;
        this.workerExecNums = conf.getInt(Constants.WORKER_EXEC_THREADS,
                Constants.defaultWorkerExecThreadNum);
        // worker thread pool executor
        this.workerExecService = ThreadUtils.newDaemonFixedThreadExecutor("Worker-Fetch-Task-Thread",workerExecNums);
        this.conf = conf;
        this.taskQueue = taskQueue;
        this.taskInstance = null;
    }

    /**
     * Check if the task runs on this worker
     * @param taskInstance
     * @param host
     * @return
     */
    private boolean checkWorkerGroup(TaskInstance taskInstance, String host){

        int taskWorkerGroupId = processDao.getTaskWorkerGroupId(taskInstance);

        if(taskWorkerGroupId <= 0){
            return true;
        }
        WorkerGroup workerGroup = processDao.queryWorkerGroupById(taskWorkerGroupId);
        if(workerGroup == null ){
            logger.info("task {} cannot find the worker group, use all worker instead.", taskInstance.getId());
            return true;
        }
        String ips = workerGroup.getIpList();
        if(StringUtils.isBlank(ips)){
            logger.error("task:{} worker group:{} parameters(ip_list) is null, this task would be running on all workers",
                    taskInstance.getId(), workerGroup.getId());
        }
        String[] ipArray = ips.split(Constants.COMMA);
        List<String> ipList =  Arrays.asList(ipArray);
        return ipList.contains(host);
    }




    @Override
    public void run() {
        while (Stopper.isRunning()){
            InterProcessMutex mutex = null;
            try {
                ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) workerExecService;
                //check memory and cpu usage and threads
                boolean runCheckFlag = OSUtils.checkResource(this.conf, false) && checkThreadCount(poolExecutor);

                Thread.sleep(Constants.SLEEP_TIME_MILLIS);

                if(!runCheckFlag) {
                    continue;
                }

                //whether have tasks, if no tasks , no need lock  //get all tasks
                List<String> tasksQueueList = taskQueue.getAllTasks(Constants.DOLPHINSCHEDULER_TASKS_QUEUE);
                if (CollectionUtils.isEmpty(tasksQueueList) && processDao.getReadyAsyncCallbackMessageCount() == 0){  //如果没有待处理的任务，或者没有confirm的回调消息的话, 5秒后再探...
                    Thread.sleep(5000);
                    continue;
                }

                // creating distributed locks, lock path /dolphinscheduler/lock/worker
                mutex = zkWorkerClient.acquireZkLock(zkWorkerClient.getZkClient(),
                        zkWorkerClient.getWorkerLockPath());
                //锁定后, 独占处理未confirm的消息, 每次最多处理10条
                try {

                    List<AsyncCallbackMsg> waitingMessages = processDao.getReadyAsyncCallbackMessages(10);
                    for(AsyncCallbackMsg item : waitingMessages){
                        if(item == null)
                            continue;

                        //修改Confirm时间，表示该节点已经处理了
                        processDao.updateAsyncCallbackMsgConfirmedState(item.getProcessInstanceId(), item.getCallbackTag());

                        //尝试将恢复的节点加入处理队列, 如果已存在则不加，如果出现异常，可能需要回退上一步的操作.
                        try {
                            if (!taskQueue.checkTaskExists(Constants.DOLPHINSCHEDULER_TASKS_QUEUE, item.getTaskQueryStr()))
                                taskQueue.add(Constants.DOLPHINSCHEDULER_TASKS_QUEUE, item.getTaskQueryStr());
                        } catch (Exception ex) {
                            //是否回退Confirm状态???
                        }
                    }
                }
                catch (Exception ex){

                }
                finally {

                }

                // task instance id str
                List<String> taskQueueStrArr = taskQueue.poll(Constants.DOLPHINSCHEDULER_TASKS_QUEUE, taskNum);

                for(String taskQueueStr : taskQueueStrArr){
                    if (StringUtils.isEmpty(taskQueueStr)) {
                        continue;
                    }

                    if (!checkThreadCount(poolExecutor)) {
                        break;
                    }

                    // get task instance id
                    taskInstId = getTaskInstanceId(taskQueueStr);

                    // mainly to wait for the master insert task to succeed
                    waitForTaskInstance();

                    taskInstance = processDao.getTaskInstanceDetailByTaskId(taskInstId);

                    // verify task instance is null
                    if (verifyTaskInstanceIsNull(taskInstance)) {
                        logger.warn("remove task queue : {} due to taskInstance is null", taskQueueStr);
                        removeNodeFromTaskQueue(taskQueueStr);
                        continue;
                    }

                    Tenant tenant = processDao.getTenantForProcess(taskInstance.getProcessInstance().getTenantId(),
                            taskInstance.getProcessDefine().getUserId());

                    // verify tenant is null
                    if (verifyTenantIsNull(tenant)) {
                        logger.warn("remove task queue : {} due to tenant is null", taskQueueStr);
                        removeNodeFromTaskQueue(taskQueueStr);

                        continue;
                    }

                    // set queue for process instance, user-specified queue takes precedence over tenant queue
                    String userQueue = processDao.queryUserQueueByProcessInstanceId(taskInstance.getProcessInstanceId());
                    taskInstance.getProcessInstance().setQueue(StringUtils.isEmpty(userQueue) ? tenant.getQueue() : userQueue);
                    taskInstance.getProcessInstance().setTenantCode(tenant.getTenantCode());

                    logger.info("worker fetch taskId : {} from queue ", taskInstId);


                    if(!checkWorkerGroup(taskInstance, OSUtils.getHost())){
                        continue;
                    }

                    // local execute path
                    String execLocalPath = getExecLocalPath();

                    logger.info("task instance  local execute path : {} ", execLocalPath);

                    // init task
                    taskInstance.init(OSUtils.getHost(),
                            new Date(),
                            execLocalPath);

                    //判断当点是否是一个call back节点
                    StringBuilder sbTag = new StringBuilder();
                    if(!isTaskInstanceReady(taskInstance, sbTag))
                    {
                        logger.info("Waiting callback ...");
                        processDao.saveEmptyAsyncCallbackMsg(taskInstance.getProcessInstanceId(), sbTag.toString(), taskQueueStr);
                        removeNodeFromTaskQueue(taskQueueStr);
                        continue;
                    }

                    // check and create Linux users
                    FileUtils.createWorkDirAndUserIfAbsent(execLocalPath,
                            tenant.getTenantCode(), logger);

                    logger.info("task : {} ready to submit to task scheduler thread",taskInstId);
                    // submit task
                    workerExecService.submit(new TaskScheduleThread(taskInstance, processDao));

                    // remove node from zk
                    removeNodeFromTaskQueue(taskQueueStr);
                }

            }catch (Exception e){
                logger.error("fetch task thread failure" ,e);
            }finally {
                AbstractZKClient.releaseMutex(mutex);
            }
        }
    }

    private Boolean isTaskInstanceReady(TaskInstance taskInstance, StringBuilder sbCallbackTag){

        //只判断PYHON节点
        if(!taskInstance.getTaskType().equals("PYTHON")){
            return true;
        }

        String taskJson = taskInstance.getTaskJson();
        if(taskJson.contains("callback_tag")){
            TaskNode taskNode = JSONObject.parseObject(taskInstance.getTaskJson(), TaskNode.class);

            String parameter = taskNode.getParams();
            Map<String, String> map = JSONUtils.toMap(parameter);

            Collection<Property> userDefParamsList = JSONObject.parseArray(map.get("localParams"), Property.class);
            for(Property item: userDefParamsList){
                if(item.getProp().equals("callback_tag")){

                    String callback_tag = item.getValue();
                    int taskInstId = taskInstance.getProcessInstanceId();

                    logger.info("Current task instance Id: {}, Current params: {}, " ,taskInstId, parameter);

                    sbCallbackTag.append(callback_tag);

                    //去系统中检查是否已经有callback
                    AsyncCallbackMsg msg = this.processDao.getAsyncCallbackMsgByKey(taskInstId, callback_tag);
                    if(msg != null && msg.isReadyForProcessing()){

                        logger.warn("Current callback is ready! process_instance_id:{}, callback_tag:{}", msg.getProcessInstanceId(), msg.getCallbackTag());
                        return true;
                    }

                    logger.warn("Current callback is not ready! process_instance_id:{}, callback_tag:{}", taskInstance.getProcessInstanceId(), callback_tag);
                    return false;
                }

            }




        }

        return true;

    }

    /**
     * remove node from task queue
     *
     * @param taskQueueStr task queue
     */
    private void removeNodeFromTaskQueue(String taskQueueStr){
        taskQueue.removeNode(Constants.DOLPHINSCHEDULER_TASKS_QUEUE, taskQueueStr);
    }

    /**
     * verify task instance is null
     * @param taskInstance
     * @return true if task instance is null
     */
    private boolean verifyTaskInstanceIsNull(TaskInstance taskInstance) {
        if (taskInstance == null ) {
            logger.error("task instance is null. task id : {} ", taskInstId);
            return true;
        }
        return false;
    }

    /**
     * verify tenant is null
     *
     * @param tenant tenant
     * @return true if tenant is null
     */
    private boolean verifyTenantIsNull(Tenant tenant) {
        if(tenant == null){
            logger.error("tenant not exists,process define id : {},process instance id : {},task instance id : {}",
                    taskInstance.getProcessDefine().getId(),
                    taskInstance.getProcessInstance().getId(),
                    taskInstance.getId());
            return true;
        }
        return false;
    }

    /**
     * get execute local path
     *
     * @return execute local path
     */
    private String getExecLocalPath(){
        return FileUtils.getProcessExecDir(taskInstance.getProcessDefine().getProjectId(),
                taskInstance.getProcessDefine().getId(),
                taskInstance.getProcessInstance().getId(),
                taskInstance.getId());
    }

    /**
     * check thread count
     *
     * @param poolExecutor pool executor
     * @return true if active count < worker exec nums
     */
    private boolean checkThreadCount(ThreadPoolExecutor poolExecutor) {
        int activeCount = poolExecutor.getActiveCount();
        if (activeCount >= workerExecNums) {
            logger.info("thread insufficient , activeCount : {} , " +
                            "workerExecNums : {}, will sleep : {} millis for thread resource",
                    activeCount,
                    workerExecNums,
                    Constants.SLEEP_TIME_MILLIS);
            return false;
        }
        return true;
    }

    /**
     * wait for task instance exists, because of db action would be delayed.
     *
     * @throws Exception exception
     */
    private void waitForTaskInstance()throws Exception{
        int retryTimes = 30;
        while (taskInstance == null && retryTimes > 0) {
            Thread.sleep(Constants.SLEEP_TIME_MILLIS);
            taskInstance = processDao.findTaskInstanceById(taskInstId);
            retryTimes--;
        }
    }

    /**
     * get task instance id
     *
     * @param taskQueueStr task queue
     * @return task instance id
     */
    private int getTaskInstanceId(String taskQueueStr){
        return Integer.parseInt(taskQueueStr.split(Constants.UNDERLINE)[3]);
    }
}