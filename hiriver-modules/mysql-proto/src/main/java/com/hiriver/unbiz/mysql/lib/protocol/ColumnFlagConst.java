package com.hiriver.unbiz.mysql.lib.protocol;

public interface ColumnFlagConst {
    int NOT_NULL_FLAG = 0x01; /* Field can't be NULL */
    int PRI_KEY_FLAG = 0x02; /* Field is part of a primary key */
    int UNIQUE_KEY_FLAG = 0x04; /* Field is part of a unique key */
    int MULTIPLE_KEY_FLAG = 0x08; /* Field is part of a key */
    int BLOB_FLAG = 0x10; /* Field is a blob */
    int UNSIGNED_FLAG = 0x20; /* Field is unsigned */
    int ZEROFILL_FLAG = 0x40; /* Field is zerofill */
    int BINARY_FLAG = 0x80; /* Field is binary */
    int ENUM_FLAG = 0x0100; /* field is an enum */
    int AUTO_INCREMENT_FLAG = 0x0200; /* field is a autoincrement field */
    int TIMESTAMP_FLAG = 0x0400; /* Field is a timestamp */
    int SET_FLAG = 0x0800; /* field is a set */
    int NO_DEFAULT_VALUE_FLAG = 0x1000; /* Field doesn't have default value */
    int ON_UPDATE_NOW_FLAG = 0x2000; /* Field is set to NOW on UPDATE */
    int NUM_FLAG = 0x8000; /* Field is num (for clients) */
}
