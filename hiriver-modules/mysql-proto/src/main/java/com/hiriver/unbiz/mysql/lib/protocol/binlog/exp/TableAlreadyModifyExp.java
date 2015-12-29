package com.hiriver.unbiz.mysql.lib.protocol.binlog.exp;

public class TableAlreadyModifyExp extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public TableAlreadyModifyExp(String message){
		super(message);
	}

}
