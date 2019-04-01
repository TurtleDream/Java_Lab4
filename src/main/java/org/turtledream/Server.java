package org.turtledream;

import com.hazelcast.core.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

import java.io.*;
import java.util.Scanner;

class ServerMsgListener implements MessageListener<String> {

    String path;

    ServerMsgListener(String path){
        this.path = path;
    }

    public void onMessage(Message<String> message) {

        String msg = message.getMessageObject();

        if(msg.equals("Backup")) {

            File file = new File(path + "/text.txt");
            file.delete();
            File file1 = new File(path + "/text2.txt");

            if (file1.exists()) file1.renameTo(new File(path + "/text.txt"));

            file1.delete();

        }

        if(msg.equals("Success")){
            File file1 = new File(path + "/text2.txt");
            if(file1.exists()) file1.delete();
        }

        System.out.println("Message received = " + msg);

    }
}

class FileListener implements MessageListener<File> {

    ITopic<String> topic_get;
    String path;

    FileListener(ITopic<String> t_g, String path){
        this.topic_get = t_g;
        this.path = path;
    }

    public void onMessage(com.hazelcast.core.Message<File> message){

        File cur = new File(path + "/text.txt");

        if(cur.exists()) {
           cur.renameTo(new File(path +"/text2.txt"));
        }

        File file = message.getMessageObject();
        BufferedWriter writer;

        try {

            writer = new BufferedWriter(new FileWriter(path + "/text.txt"));
            FileInputStream fis = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];

            fis.read(data);
            fis.close();

            writer.write(new String(data));
            writer.close();

            System.out.println("Message received = " + file.toString());
            topic_get.publish("Delivered!");

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

public class Server extends AbstractVerticle {

    public static HazelcastInstance hz;
    public static ITopic<File> topic_send;
    public static ITopic<String> topic_get;
    public static ITopic<String> topic_backup;

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        System.out.println("path:");
        String path = scanner.nextLine();

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new Server());
        hz = Hazelcast.newHazelcastInstance();

        topic_send = hz.getTopic("topic_send");
        topic_get = hz.getTopic("topic_get");
        topic_backup = hz.getTopic("topic_backup");

        topic_backup.addMessageListener(new ServerMsgListener(path));
        topic_send.addMessageListener(new FileListener(topic_get, path));

    }
}
