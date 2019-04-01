package org.turtledream;

import com.hazelcast.core.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.File;

class MainMsgListener implements MessageListener<String> {

    IAtomicLong counter;

    MainMsgListener (IAtomicLong counter){
        this.counter = counter;
    }

    public void onMessage(Message<String> message) {

        String msg = message.getMessageObject();

        if(msg.equals("Delivered!")) {
            counter.addAndGet(1);
        }

        System.out.println("Message received = " + msg);

    }
}

public class Main extends AbstractVerticle{

    public static HazelcastInstance hz;
    public static ITopic<File> topic_send;
    public static ITopic<String> topic_get;
    public static ITopic<String> topic_backup;
    public static IAtomicLong counter;

    @Override
    public void start(){

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create().setUploadsDirectory("upls"));

        router.route("/").handler(routingContext -> {
            routingContext.response().putHeader("content-type", "text/html").end(
                    "<form action=\"/form\" method=\"post\" enctype=\"multipart/form-data\">\n" +
                            "    <div>\n" +
                            "        <label for=\"name\">Select a file:</label>\n" +
                            "        <input type=\"file\" name=\"file\">\n" +
                            "    </div>\n" +
                            "    <div class=\"button\">\n" +
                            "        <button type=\"submit\">Send</button>\n" +
                            "    </div>" +
                            "</form>"
            );
        });

        router.route("/form").handler(ctx -> {

            for (FileUpload fileUpload : ctx.fileUploads()) {

                File file = new File(fileUpload.uploadedFileName());
                topic_send.publish(file);

                long lastTime = System.currentTimeMillis();
                long curTime = lastTime;

                while((counter.get() != hz.getCluster().getMembers().size() - 1) && (lastTime - curTime < 5000)){
                    lastTime = System.currentTimeMillis();
                }

                if(counter.get() == hz.getCluster().getMembers().size() - 1){
                    topic_backup.publish("Success");
                    file.delete();
                    counter.set(0);
                    ctx.response().putHeader("Content-Type", "text/html").end("Success");
                }
                else{
                    topic_backup.publish("Backup");
                    file.delete();
                    counter.set(0);
                    ctx.response().putHeader("Content-Type", "text/html").end("Fail");
                }
            }
        });

        vertx.createHttpServer().requestHandler(router).listen(8080);
    }

    public static void main(String[] args) {

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new Main());
        hz = Hazelcast.newHazelcastInstance();

        counter = hz.getAtomicLong("counter");

        topic_send = hz.getTopic("topic_send");
        topic_get = hz.getTopic("topic_get");
        topic_backup = hz.getTopic("topic_backup");

        topic_get.addMessageListener(new MainMsgListener(counter));

    }
}
