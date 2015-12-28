package com.hiriver.unbiz.mysql.lib.protocol.binlog;

public interface BinlogEventType {
    int UNKNOWN_EVENT = 0x00;

    int START_EVENT_V3 = 0x01;

    int QUERY_EVENT = 0x02;

    int STOP_EVENT = 0x03;

    int ROTATE_EVENT = 0x04;

    int INTVAR_EVENT = 0x05;

    int LOAD_EVENT = 0x06;

    int SLAVE_EVENT = 0x07;

    int CREATE_FILE_EVENT = 0x08;

    int APPEND_BLOCK_EVENT = 0x09;

    int EXEC_LOAD_EVENT = 0x0a;

    int DELETE_FILE_EVENT = 0x0b;

    int NEW_LOAD_EVENT = 0x0c;

    int RAND_EVENT = 0x0d;

    int USER_VAR_EVENT = 0x0e;

    int FORMAT_DESCRIPTION_EVENT = 0x0f;

    int XID_EVENT = 0x10;

    int BEGIN_LOAD_QUERY_EVENT = 0x11;

    int EXECUTE_LOAD_QUERY_EVENT = 0x12;

    int TABLE_MAP_EVENT = 0x13;

    int WRITE_ROWS_EVENTv0 = 0x14;

    int UPDATE_ROWS_EVENTv0 = 0x15;

    int DELETE_ROWS_EVENTv0 = 0x16;

    int WRITE_ROWS_EVENTv1 = 0x17;

    int UPDATE_ROWS_EVENTv1 = 0x18;

    int DELETE_ROWS_EVENTv1 = 0x19;

    int INCIDENT_EVENT = 0x1a;

    int HEARTBEAT_EVENT = 0x1b;

    int IGNORABLE_EVENT = 0x1c;

    int ROWS_QUERY_EVENT = 0x1d;

    int WRITE_ROWS_EVENTv2 = 0x1e;

    int UPDATE_ROWS_EVENTv2 = 0x1f;

    int DELETE_ROWS_EVENTv2 = 0x20;

    int GTID_EVENT = 0x21;

    int ANONYMOUS_GTID_EVENT = 0x22;

    int PREVIOUS_GTIDS_EVENT = 0x23;
}
