package javacourse.net;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Server {
    public static void main(String[] args) throws IOException {
        ServerSocket socket = new ServerSocket(25225, 2000);

        Map<String, Greetable> handlers = loadHandlers();

        System.out.println("Server is STARTED!" + LocalDateTime.now());

        while (true) {
            Socket client = socket.accept();
            new SimpleServer(client, handlers).start();
        }
    }

    private static Map<String, Greetable> loadHandlers() {
        Map<String, Greetable> result = new HashMap<>();
        try (InputStream is = Server.class.getClassLoader()
                .getResourceAsStream("server.properties")) {
            Properties properties = new Properties();
            properties.load(is);

            for (Object command : properties.keySet()) {
                String className = properties.getProperty(command.toString());
                Class<Greetable> cl = (Class<Greetable>) Class.forName(className);

                Greetable handler = cl.getConstructor().newInstance();
                result.put(command.toString(), handler);


            }

        } catch (Exception ex) {
            ex.printStackTrace();


        }
        return result;

    }
}

class SimpleServer extends Thread {

    private Socket client;
    private Map<String, Greetable> handlers;

    public SimpleServer(Socket client, Map<String, Greetable> handlers) {
        this.client = client;
        this.handlers = handlers;
    }

    public void run() {
        handleRequest();
    }

    private void handleRequest() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));

            StringBuilder sb = new StringBuilder("Hello, ");
            String request = br.readLine();
            String[] lines = request.split("\\s+");
            String userName = lines[1];
            String command = lines[0];
            System.out.println("Server got string1:" + command);
            System.out.println("Server got string2:" + userName);
//            Thread.sleep(2000);

            String response = buildResponse(command, userName);
            sb.append(userName);
            bw.write(response);
            bw.newLine();
            bw.flush();

            br.close();
            bw.close();

            client.close();

        } catch (Exception ex) {
            ex.printStackTrace(System.out);

        }
    }

    private String buildResponse(String command, String userName) {
//        switch (command) {
//            case "HELLO":
//                return "Hello, " + userName;
//            case "MORNING":
//                return "Good morning, " + userName;
//            case "DAY":
//                return "Good day, " + userName;
//            case "EVENING":
//                return "Good evening, " + userName;
//            default:
//                return "Hi, " + userName;
//        }

        Greetable handler = handlers.get(command);
        if (handler != null) {
            return handler.buildResponse(userName);
        }
        return "Hello, " + userName;
    }
}