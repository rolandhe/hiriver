package com.hiriver.unbiz.mysql.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.unbiz.mysql.lib.protocol.ERRPacket;
import com.hiriver.unbiz.mysql.lib.protocol.OKPacket;
import com.hiriver.unbiz.mysql.lib.protocol.PacketHeader;
import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.connect.HandShakeResponseV41;
import com.hiriver.unbiz.mysql.lib.protocol.connect.HandShakeV10;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PacketTool;
import com.hiriverunbiz.mysql.lib.exp.BaseExecuteException;
import com.hiriverunbiz.mysql.lib.exp.HandShakeException;
import com.hiriverunbiz.mysql.lib.exp.NetworkException;
import com.hiriverunbiz.mysql.lib.exp.NotExpectPayloadException;
import com.hiriverunbiz.mysql.lib.exp.PeerResetNetworkException;
import com.hiriverunbiz.mysql.lib.exp.UnOpenedSocket;

/**
 * 实现与mysql连接的抽象类
 * 
 * @author hexiufeng
 * 
 */
public abstract class AbstractBlockingTransport implements BlockingTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBlockingTransport.class);

    private String host;
    private int port;
    private String userName;
    private String password;
    private String defDbName;
    private TransportConfig transportConfig = new TransportConfig();

    /**
     * 虽然BlockingTransport是<b>非线程安全</b>的,但BlockingTransport可能被池化，会被多个线程 使用，使用<b>volatile关键词</b>解决线程不可见问题。
     */
    private volatile SocketHolder socketHolder;
    private volatile boolean isOpened = false;
    private volatile boolean isOpeningPhrase = false;

    /**
     * BlockingTransport是线程不安全的，因此一个BlockingTransport在某一时刻只能被一个线程使用。在一个
     * 线程的生命周期内header对象能被重复的使用。使用<b>final关键词</b>保证该header字段本身不能被线程修改，
     * 这保证了线程安全，同时header的内容没有必要被其他线程获取，只要本线程可见即可，因此PacketHeader类没必要做 线程安全处理。
     * 
     */
    private final PacketHeader header = new PacketHeader();

    protected SocketReadTimeoutHanlder readTimeoutHanlder = new SocketReadTimeoutHanlder() {

        @Override
        public void handle(String message, Exception e) {
            throw new NetworkException(message, e);
        }

    };

    /**
     * 底层socket的占位符对象，用于保存当前Transport的底层socket和输入、输出流。
     * 
     * <p>
     * 它是一个非可变的类型，因此是线程安全的。
     * </p>
     * 
     * @author hexiufeng
     * 
     */
    protected static class SocketHolder {
        final Socket socket;
        final InputStream in;
        final OutputStream out;

        /**
         * 构造方法,之所以传入socket.getInputStream和socket.getOutStream,是因为不想在构造方法抛出异常
         * 
         * @param socket socket instance
         * @param in socket.getInputStream
         * @param out socket.getOutStream
         */
        public SocketHolder(Socket socket, InputStream in, OutputStream out) {
            this.socket = socket;
            this.in = in;
            this.out = out;
        }
    }

    protected AbstractBlockingTransport() {

    }

    /**
     * 构造函数
     * 
     * @param host host
     * @param port port
     * @param userName user name
     * @param password password
     */
    protected AbstractBlockingTransport(String host, int port, String userName, String password) {
        this(host, port, userName, password, null);
    }

    /**
     * 构造函数
     * 
     * @param host host
     * @param port port
     * @param userName user name
     * @param password password
     * @param defDbName db name
     */
    protected AbstractBlockingTransport(String host, int port, String userName, String password, String defDbName) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.defDbName = defDbName;
    }

    /**
     * 获取实现类的logger对象
     * 
     * @return 实现类的logger对象
     */
    protected abstract Logger getSubClassLogger();

    /**
     * 初始链接.
     * <p>
     * 在连接刚被建立后可能需要执行一系列的初始化sql。
     * </p>
     * 
     * @param sql 执行初始化连接的sql语句
     */
    protected abstract void intiTransport(String sql);

    /**
     * 获取当前有效的日志记录对象
     * 
     * @return Logger
     */
    private Logger getLogger() {
        return getSubClassLogger() == null ? LOGGER : getSubClassLogger();
    }

    /**
     * 打开socket并初始化socket的属性
     */
    private void openSocket() {
        Socket socket = new Socket();
        InetSocketAddress address = new InetSocketAddress(host, port);
        try {
            socket.connect(address, transportConfig.getConnectTimeout());
        } catch (IOException e) {
            Logger logger = getLogger();
            if (logger.isErrorEnabled()) {
                getLogger().error("connect failed" + host + ":" + port, e);
            }
            IOUtils.closeQuietly(socket);
            throw new NetworkException(e);
        }
        try {
            socket.setKeepAlive(transportConfig.isKeepAlive());
            socket.setSoTimeout(transportConfig.getSoTimeout());
            if (transportConfig.getRecieveBufferSize() > 0) {
                socket.setReceiveBufferSize(transportConfig.getRecieveBufferSize());
            }
            if (transportConfig.getSendBufferSize() > 0) {
                socket.setSendBufferSize(transportConfig.getSendBufferSize());
            }
            socketHolder = new SocketHolder(socket, socket.getInputStream(), socket.getOutputStream());
        } catch (IOException e) {
            safeCloseSocket();
            throw new NetworkException("get in/out stream error from " + host + ":" + port, e);
        }
    }

    /**
     * 连接完成后，与mysql进行握手验证
     */
    private void doHandShake() {
        // read handshake
        HandShakeV10 handShake = new HandShakeV10();
        handShake.parse(readResponsePayload());
        // send handshake response request
        HandShakeResponseV41 shakeResp = new HandShakeResponseV41(handShake, header.getSequenceId() + 1);
        shakeResp.setUserName(userName);
        shakeResp.setPassword(password); //
        shakeResp.setDbName(defDbName);
        writeRequest(shakeResp);

        // read HandShakeResponseV41 request's response
        byte[] buffer = this.readResponsePayload();
        if (PacketTool.isErrPackete(buffer)) {
            ERRPacket ep = new ERRPacket();
            ep.parse(buffer);
            safeCloseSocket();
            throw new HandShakeException(ep.getErrorMessage());
        }
        if (PacketTool.isChangeAuthPackete(buffer)) {
            throw new HandShakeException("just support mysql 5.1 or above.");
        }

        OKPacket okPacket = new OKPacket();
        okPacket.parse(buffer);

        getLogger().info("handshake success.");
    }

    /**
     * 检查当前响应数据是否是ERROR Packet数据
     * 
     * @param buffer 响应数据
     */
    protected void checkErrPacket(byte[] buffer) {
        if (PacketTool.isErrPackete(buffer)) {
            ERRPacket ep = new ERRPacket();
            ep.parse(buffer);
            throw new NotExpectPayloadException(ep.getErrorMessage());
        }
    }

    /**
     * 安全的关闭连接
     */
    private void safeCloseSocket() {
        if (socketHolder == null) {
            return;
        }
        if (socketHolder.socket != null) {
            try {
                socketHolder.socket.close();
            } catch (IOException e) {
                // igore
            } finally {
                socketHolder = null;
            }
        }
    }

    @Override
    public void open() throws NetworkException, HandShakeException {
        openSocket();
        try {
            isOpeningPhrase = true;
            doHandShake();
            initOpenedTransport();
        } finally {
            isOpeningPhrase = false;
        }
        isOpened = true;
    }

    /**
     * 与msyql完成握手后对连接进行初始化
     */
    private void initOpenedTransport() {
        if (transportConfig.getInitSqlList() == null) {
            return;
        }
        try {
            for (String initSQL : transportConfig.getInitSqlList()) {
                intiTransport(initSQL);
            }
        } catch (BaseExecuteException e) {
            safeCloseSocket();
            throw e;
        }
    }

    @Override
    public void close() {
        safeCloseSocket();
        isOpened = false;
    }

    @Override
    public boolean isOpen() {
        return isOpened;
    }

    @Override
    public byte[] readResponsePayload() throws NetworkException, PeerResetNetworkException, UnOpenedSocket {
        checkOpen();
        byte[] buffer = readBytes(header.getExpectLen());
        header.parse(buffer);
        return readBytes(header.getPayloadLen());
    }

    @Override
    public void writeRequest(Request request) throws NetworkException, UnOpenedSocket {
        checkOpen();
        try {
            this.socketHolder.out.write(request.toByteArray());
        } catch (IOException e) {
            close();
            throw new NetworkException("write data to msyql error: " + host + ":" + port, e);
        }
    }

    /**
     * 检查连接是否已经被打开
     */
    private void checkOpen() {
        if (!isOpened && !isOpeningPhrase) {
            throw new UnOpenedSocket("unopened socket error: " + host + ":" + port);
        }
    }

    /**
     * 读取指定大小的数据块
     * 
     * @param len 期望读取数据的字节数
     * @return 数据块
     */
    private byte[] readBytes(int len) {
        byte[] buffer = new byte[len];
        int count = 0;

        while (count < buffer.length) {
            int size = 0;
            try {
                size = socketHolder.in.read(buffer, count, buffer.length - count);
            } catch (SocketTimeoutException e) {
                readTimeoutHanlder.handle("read error from " + host + ":" + port, e);
            } catch (IOException e) {
                throw new NetworkException("read error from " + host + ":" + port, e);
            }
            if (size == -1) {
                throw new PeerResetNetworkException("connect reset:" + host + ":" + port);
            }
            count += size;
        }
        return buffer;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getDefDbName() {
        return defDbName;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDefDbName(String defDbName) {
        this.defDbName = defDbName;
    }

    public TransportConfig getTransportConfig() {
        return transportConfig;
    }

    public void setTransportConfig(TransportConfig transportConfig) {
        this.transportConfig = transportConfig;
    }
}
