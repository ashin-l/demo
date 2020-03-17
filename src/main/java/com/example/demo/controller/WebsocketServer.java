package com.example.demo.controller;

import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;

import com.example.demo.service.kafka.KafkaConsumerRunner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ServerEndpoint("/ws/1")
public class WebsocketServer {
    private static Map<String, Session> clients = new ConcurrentHashMap<>();
    private static KafkaConsumerRunner c;

    @OnOpen
    public void onOpen(Session session) {
        // System.out.println(en.toString());
        System.out.println("有新的客户端连接了: " + session.getId());
        // 将新用户存入在线的组

        // System.out.println("conf:" + Constant.conf.getName());
        clients.put(session.getId(), session);
        if (clients.size() == 1) {
            try {
                c = new KafkaConsumerRunner();
                new Thread(c).start();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }

    /**
     * 客户端关闭
     * 
     * @param session session
     */
    @OnClose
    public void onClose(Session session) {
        System.out.println("有用户断开了, id为:" + session.getId());
        // 将掉线的用户移除在线的组里
        clients.remove(session.getId());
        if (clients.size() == 0) {
            c.shutdown();
        }
    }

    /**
     * 发生错误
     * 
     * @param throwable e
     */
    @OnError
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    /**
     * 收到客户端发来消息
     * 
     * @param message 消息对象
     */
    @OnMessage
    public void onMessage(String message) {
        System.out.println("服务端收到客户端发来的消息: " + message);
        this.sendAll(message);
    }

    /**
     * 群发消息
     * 
     * @param message 消息内容
     */
    public static void sendAll(String message) {
        for (Map.Entry<String, Session> sessionEntry : clients.entrySet()) {
            sessionEntry.getValue().getAsyncRemote().sendText(message);
        }
    }

}
