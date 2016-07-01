package com.hiriver.unbiz.msyql;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.hiriver.unbiz.mysql.lib.ResultContentReader;
import com.hiriver.unbiz.mysql.lib.protocol.ERRPacket;
import com.hiriver.unbiz.mysql.lib.protocol.OKPacket;
import com.hiriver.unbiz.mysql.lib.protocol.PacketHeader;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.connect.HandShakeResponseV41;
import com.hiriver.unbiz.mysql.lib.protocol.connect.HandShakeV10;
import com.hiriver.unbiz.mysql.lib.protocol.text.FieldListCommandResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandFieldListRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PacketTool;

public class Connection {

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 3308);
        socket.connect(address, 2000);

        final PacketHeader header = new PacketHeader();
        readAndParse(socket.getInputStream(), header, header.getExpectLen());

        HandShakeV10 handShake = new HandShakeV10();
        readAndParse(socket.getInputStream(), handShake, header.getPayloadLen());
        HandShakeResponseV41 shakeResp = new HandShakeResponseV41(handShake, 1,0);

        // shakeResp.setSequenceId(shakeResp.getSequenceId());
        shakeResp.setUserName("root");
        shakeResp.setPassword("123"); //
        shakeResp.setDbName("creditchina");
        socket.getOutputStream().write(shakeResp.toByteArray());
        final InputStream in = socket.getInputStream();
        readAndParse(in, header, header.getExpectLen());
        byte[] buffer = readBytes(in, header.getPayloadLen());
        if (PacketTool.isErrPackete(buffer)) {
            ERRPacket ep = new ERRPacket();
            ep.parse(buffer);
            throw new RuntimeException();
        }
        OKPacket okPacket = new OKPacket();
        okPacket.parse(buffer);

        // querySelect(socket, header, in);
        queryCommand(socket, header, in);
        socket.close();
    }

    private static void queryCommand(Socket socket, final PacketHeader header, final InputStream in)
            throws IOException, Exception, UnsupportedEncodingException {
        byte[] buffer;
        TextCommandFieldListRequest fieldListRequest = new TextCommandFieldListRequest("test");

        socket.getOutputStream().write(fieldListRequest.toByteArray());
        readAndParse(in, header, header.getExpectLen());
        buffer = readBytes(in, header.getPayloadLen());
        if (PacketTool.isErrPackete(buffer)) {
            ERRPacket ep = new ERRPacket();
            ep.parse(buffer);
            throw new RuntimeException();
        }
        if (PacketTool.isOkPackete(buffer)) {
            OKPacket okPacket = new OKPacket();
            okPacket.parse(buffer);
            return;
        }
        FieldListCommandResponse fieldResp = new FieldListCommandResponse(new ResultContentReader() {

            @Override
            public byte[] readNextPacketPayload() {
                try {
                    readAndParse(in, header, header.getExpectLen());
                    return readBytes(in, header.getPayloadLen());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        });
        fieldResp.parse(buffer);
        System.out.println("aa");
    }

    public static void querySelect(Socket socket, final PacketHeader header, final InputStream in)
            throws IOException, Exception, UnsupportedEncodingException {
        byte[] buffer;
        TextCommandQueryRequest query = new TextCommandQueryRequest("set names utf8");

        socket.getOutputStream().write(query.toByteArray());
        readAndParse(in, header, header.getExpectLen());
        buffer = readBytes(in, header.getPayloadLen());
        if (PacketTool.isErrPackete(buffer)) {
            ERRPacket ep = new ERRPacket();
            ep.parse(buffer);
            throw new RuntimeException();
        }
        if (PacketTool.isOkPackete(buffer)) {
            OKPacket okPacket = new OKPacket();
            okPacket.parse(buffer);
            return;
        }
        TextCommandQueryResponse queryResp = new TextCommandQueryResponse(new ResultContentReader() {

            @Override
            public byte[] readNextPacketPayload() {
                try {
                    readAndParse(in, header, header.getExpectLen());
                    return readBytes(in, header.getPayloadLen());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        });
        queryResp.parse(buffer);
        String data = queryResp.getRowList().get(0).getValueList().get(0).getValueAsString();
        System.out.println(data);
    }

    private static void readAndParse(InputStream in, Response response, int len) throws Exception {
        byte[] buff = readBytes(in, len);
        if (buff[0] == -1) {
            ERRPacket ep = new ERRPacket();
            ep.parse(buff);
        }

        response.parse(buff);
    }

    private static byte[] readBytes(InputStream in, int len) throws Exception {
        byte[] buffer = new byte[len];
        int count = 0;

        while (count < buffer.length) {
            int size = in.read(buffer, count, buffer.length - count);
            if (size == -1) {
                throw new RuntimeException("connect reset");
            }
            count += size;
        }
        return buffer;
    }

}
