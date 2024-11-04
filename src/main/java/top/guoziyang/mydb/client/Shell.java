package top.guoziyang.mydb.client;

import java.util.Scanner;

/**
 * 接收用户输入，执行
 */
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    // 启动 shell
    public void run() {
        Scanner sc = new Scanner(System.in);
        try {
            // 循环接收用户输入
            while(true) {
                System.out.print(":> ");

                // 读取用户输入
                String statStr = sc.nextLine();

                // 判断是否退出shell
                if("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    // 执行 sql
                    byte[] res = client.execute(statStr.getBytes());
                    // 输出执行结果
                    System.out.println(new String(res));
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

            }
        } finally {
            sc.close();
            client.close();
        }
    }
}
