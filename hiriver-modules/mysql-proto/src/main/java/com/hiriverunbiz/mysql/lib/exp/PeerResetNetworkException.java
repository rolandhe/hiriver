package com.hiriverunbiz.mysql.lib.exp;

/**
 * 底层socket被对方关闭异常。
 * 
 * @author hexiufeng
 *
 */
public class PeerResetNetworkException extends BaseExecuteException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public PeerResetNetworkException(String message) {
        super(message);
    }

}
