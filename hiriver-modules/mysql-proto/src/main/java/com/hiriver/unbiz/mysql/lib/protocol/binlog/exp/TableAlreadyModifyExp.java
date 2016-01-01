package com.hiriver.unbiz.mysql.lib.protocol.binlog.exp;

/**
 * 表结构已经被修改异常，当接收到的binlog事件描述的列总数大于当前数据库
 * 中表的列数时，抛出该异常，表示表结构已经发生变化，将无法匹配到正确的列名
 * 
 * @author hexiufeng
 *
 */
public class TableAlreadyModifyExp extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public TableAlreadyModifyExp(String message){
		super(message);
	}

}
