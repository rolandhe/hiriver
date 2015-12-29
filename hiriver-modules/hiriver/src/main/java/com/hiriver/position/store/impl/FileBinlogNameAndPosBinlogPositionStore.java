package com.hiriver.position.store.impl;

import com.hiriver.position.store.BinlogPositionStore;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogFileBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

public class FileBinlogNameAndPosBinlogPositionStore extends AbstractFileBinlogPositionStore implements BinlogPositionStore  {

	@Override
	protected BinlogPosition createBinlogPosition(String line) {
		String[] array = line.split(":");
		return new BinlogFileBinlogPosition(array[0],Long.parseLong(array[1]));
	}

}
