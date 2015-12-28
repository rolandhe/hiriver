package com.hiriver.unbiz.mysql.lib;

import com.hiriver.unbiz.mysql.lib.protocol.text.FieldListCommandResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandFieldListRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;

/**
 * 支持msyql Text Query协议的连接.
 * 
 * <p>
 * Text Query指的是执行sql语句的协议
 * </p>
 * 
 * @author hexiufeng
 *
 */
public interface TextProtocolBlockingTransport extends BlockingTransport {
    /**
     * 执行没有返回记录的sql命令
     * 
     * @param sql sql命令
     * @return 影响的记录数
     */
    int executeSQL(String sql);

    /**
     * 执行查询sql，返回结果集合
     * 
     * @param sql sql命令
     * @return TextCommandQueryResponse 结果集合
     */
    TextCommandQueryResponse execute(String sql);

    /**
     * 显示指的表的字段定义
     * 
     * @param fieldListRequest 字段定义命令
     * @return FieldListCommandResponse
     */
    FieldListCommandResponse showFieldList(TextCommandFieldListRequest fieldListRequest);
}
