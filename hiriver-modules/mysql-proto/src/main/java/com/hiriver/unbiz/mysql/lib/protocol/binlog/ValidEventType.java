package com.hiriver.unbiz.mysql.lib.protocol.binlog;

/**
 * 描述有效事件的类型。<br>
 * 
 * <ul>
 * <li>开始事务</li>
 * <li>事务提交</li>
 * <li>事务回滚</li>
 * <li>描述GTID的事件</li>
 * <li>行数据事件</li>
 * </ul>
 * 
 * @author hexiufeng
 *
 */
public enum ValidEventType {
    TRAN_BEGIN,TRANS_COMMIT,TRANS_ROLLBACK,GTID,ROW;
}
