package edu.javacourse.net;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;

public class Server {
    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket socket = new ServerSocket(25225, 2000);

        System.out.println("Server is STARTED!" + LocalDateTime.now());

        while (true) {
            Socket client = socket.accept();
            new SimpleServer(client).start();
        }
    }

}

class SimpleServer extends Thread {

    private Socket client;

    public SimpleServer (Socket client) {
        this.client = client;
    }

    public void run() {
        handleRequest();
    }

    private void handleRequest() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));

            StringBuilder sb = new StringBuilder("Hello, ");
            String userName = br.readLine();
            System.out.println("Server got string:" + userName);
            Thread.sleep(2000);

            sb.append(userName);
            bw.write(sb.toString());
            bw.newLine();
            bw.flush();

            br.close();
            bw.close();

            client.close();

        }catch(Exception ex) {
            ex.printStackTrace(System.out);

        }
    }
}