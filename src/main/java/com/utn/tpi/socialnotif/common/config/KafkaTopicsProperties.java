package com.utn.tpi.socialnotif.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.kafka.topics")
public class KafkaTopicsProperties {

    private String notificationEvents = "notification-events";
    private String chatEvents = "chat-events";
    private String mailEvents = "mail-events";
    private String moderationEvents = "moderation-events";

    public String getNotificationEvents() {
        return notificationEvents;
    }

    public void setNotificationEvents(String notificationEvents) {
        this.notificationEvents = notificationEvents;
    }

    public String getChatEvents() {
        return chatEvents;
    }

    public void setChatEvents(String chatEvents) {
        this.chatEvents = chatEvents;
    }

    public String getMailEvents() {
        return mailEvents;
    }

    public void setMailEvents(String mailEvents) {
        this.mailEvents = mailEvents;
    }

    public String getModerationEvents() {
        return moderationEvents;
    }

    public void setModerationEvents(String moderationEvents) {
        this.moderationEvents = moderationEvents;
    }
}
