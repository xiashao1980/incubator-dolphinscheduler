package org.apache.dolphinscheduler.dao.entity;


import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableLogic;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

/**
 * The async callback message
 */
@Data
@TableName("t_ds_async_callback_msg")
public class AsyncCallbackMsg {

    @TableField("process_instance_id")
    private int processInstanceId;

    @TableField("callback_tag")
    private String callbackTag;

    @TableField("result_code")
    private String resultCode;

    @TableField("result_msg")
    private String resultMsg;

    @TableField("result_overload")
    private String resultOverload;

    @TableField("callback_time")
    private Date callbackTime;

    @TableField("confirm_time")
    private Date confirmTime;

    @TableField("is_valid")
    private Boolean isValid;


    @TableField("task_query_str")
    private String taskQueryStr;

    /*
    返回是否已经有回调，并且未处理
     */
    public Boolean isReadyForProcessing()
    {
        return getValid() && getCallbackTime() != null && getConfirmTime() != null;
    }


    public int getProcessInstanceId() {
        return processInstanceId;
    }

    public void setProcessInstanceId(int processInstanceId) {
        this.processInstanceId = processInstanceId;
    }

    public String getCallbackTag() {
        return callbackTag;
    }

    public void setCallbackTag(String callbackTag) {
        this.callbackTag = callbackTag;
    }

    public String getResultCode() {
        return resultCode;
    }

    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }

    public String getResultMsg() {
        return resultMsg;
    }

    public void setResultMsg(String resultMsg) {
        this.resultMsg = resultMsg;
    }

    public String getResultOverload() {
        return resultOverload;
    }

    public void setResultOverload(String resultOverload) {
        this.resultOverload = resultOverload;
    }

    public Date getCallbackTime() {
        return callbackTime;
    }

    public void setCallbackTime(Date callbackTime) {
        this.callbackTime = callbackTime;
    }

    public Date getConfirmTime() {
        return confirmTime;
    }

    public void setConfirmTime(Date confirmTime) {
        this.confirmTime = confirmTime;
    }

    public Boolean getValid() {
        return isValid;
    }

    public void setValid(Boolean valid) {
        isValid = valid;
    }

    public String getTaskQueryStr() {
        return taskQueryStr;
    }

    public void setTaskQueryStr(String taskQueryStr) {
        this.taskQueryStr = taskQueryStr;
    }
}
