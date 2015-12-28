package com.hiriver.unbiz.mysql.lib.protocol.binlog;

public enum ValidEventType {
    TRAN_BEGIN,TRANS_COMMIT,TRANS_ROLLBACK,GTID,ROW;
}
