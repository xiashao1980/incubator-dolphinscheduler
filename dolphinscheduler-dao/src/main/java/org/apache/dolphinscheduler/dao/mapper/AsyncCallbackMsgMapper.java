package org.apache.dolphinscheduler.dao.mapper;

import org.apache.dolphinscheduler.dao.entity.AsyncCallbackMsg;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;


public interface AsyncCallbackMsgMapper extends BaseMapper<AsyncCallbackMsg> {

    /**
     * query message info by process instance id and callback tag
     * @param process_inst_id processId instance id
     * @param tag  callback tag
     * @return process instance
     */
    AsyncCallbackMsg getAsyncCallbackMsgByKey(@Param("process_inst_id") int process_inst_id, @Param("tag") String tag);

    /**
     * Get recently callback message items
     * @param top_limit the limit
     * @return The list of ready task(but not confirmed)
     */
    List<AsyncCallbackMsg> getReadyAsyncCallbackMessages(@Param("top_limit") int top_limit);

    /**F
     * Update confirm time field of Async callback message
     * @param process_inst_id process instance id
     * @param tag the callback tag
     */
    void updateConfirmedState(@Param("process_inst_id") int process_inst_id, @Param("tag") String tag);

    /**
     * update valid status by process instance id and callback tag
     * @param process_inst_id processId instance id
     * @param tag  callback tag
     * @param valid  true or false for valid status
     */
    void updateValidState(@Param("process_inst_id") int process_inst_id, @Param("tag") String tag, @Param("valid") Boolean valid);

    /**
     * save current callback result to db
     * @param process_inst_id process instance id
     * @param tag  callback tag
     * @param code callback result code, 200 for successfully,
     * @param msg  callback result message
     * @param overload  callback overload information, just like a json or other data
     */
    void updateCallbackResult(@Param("process_inst_id") int process_inst_id, @Param("tag") String tag, @Param("code") String code, @Param("msg") String msg, @Param("overload") String overload);

    /**
     * create a empty record for this callback
     * @param process_inst_id process instance id
     * @param tag callback tag
     */
    void saveEmptyAsyncCallbackMsg(@Param("process_inst_id") int process_inst_id, @Param("tag") String tag, @Param("task_query_str") String task_query_str);

    /**
     * 查询是否有已回调，但未处理的异步消息(callback_time不为空，表示已经有回调, confirm_time为空，表示未把任务加回队列)
     * @return a count of ready task
     */
    int getReadyAsyncCallbackMessageCount();

}
