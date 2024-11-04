package top.guoziyang.mydb.backend.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import top.guoziyang.mydb.backend.tbm.TableManager;
import top.guoziyang.mydb.transport.Encoder;
import top.guoziyang.mydb.transport.Package;
import top.guoziyang.mydb.transport.Packager;
import top.guoziyang.mydb.transport.Transporter;

/**
 * 服务端，接收请求
 */
public class Server {
    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            // 监听端口
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        // 创建线程池，管理处理客户端请求的线程



        ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                10,       // 核心线程数 10
                20,                     // 最大线程数 20
                1L,                     // 空闲线程存活时间
                TimeUnit.SECONDS,       // 时间单位
                new ArrayBlockingQueue<>(100),  //等待队列，容量为100的阻塞队列
                new ThreadPoolExecutor.CallerRunsPolicy()); // 拒绝策略：如果任务无法被线程池处理，则由提交任务的线程自己运行该任务。这种策略可以有效地减轻对线程池的压力。
        try {
            // 循环等待，处理客户端连接
            while(true) {
                // 接收连接
                Socket socket = ss.accept();
                // 新建 HandleSocket 处理请求
                Runnable worker = new HandleSocket(socket, tbm);
                // 线程池执行
                tpe.execute(worker);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {}
        }
    }
}

/**
 * 多线程
 */
class HandleSocket implements Runnable {
    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        // 获取并打印客户端相关信息
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());

        // 创建 packger ，处理数据的打包和解包
        Packager packager = null;
        try {
            // 传输器
            Transporter t = new Transporter(socket);
            // 编解码器
            Encoder e = new Encoder();
            packager = new Packager(t, e);
        } catch(IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        // 创建执行器来执行 sql
        Executor exe = new Executor(tbm);
        while(true) {
            Package pkg = null;
            try {
                // 从客户端读数据包
                pkg = packager.receive();
            } catch(Exception e) {
                break;
            }

            // 解码 sql 字符数组
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                // 执行 sql，得到结果
                result = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }
            // 包装并返回结果
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}