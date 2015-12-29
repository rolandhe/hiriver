package com.hiriver.position.store.impl;

import com.hiriver.position.store.BinlogPositionStore;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

public class FileGtIdBinlogPositionStore extends AbstractFileBinlogPositionStore implements BinlogPositionStore {

	@Override
	protected BinlogPosition createBinlogPosition(String line) {
		return new GTidBinlogPosition(line);
	}

}
