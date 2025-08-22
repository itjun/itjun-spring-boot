package io.itjun.rabbitmq.delay;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cn.cerc.db.core.Datetime;

@RequestMapping("rabbitmq")
@RestController
public class RabbitMQMsgController {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQMsgController.class);

    @Autowired
    private DelayMessageSender sender;

    @RequestMapping("sendmsg")
    public void sendMsg(String msg, Integer delayType) {
        log.info("当前时间：{},收到请求，msg:{},delayType:{}", new Datetime(), msg, delayType);
        sender.sendMsg(msg, Objects.requireNonNull(DelayTypeEnum.getDelayTypeEnumByValue(delayType)));
    }

    @RequestMapping("deadMsg")
    public void deadMsg(String msg, Integer delayTime) {
        log.info("当前时间：{},收到请求，msg:{},delayTime:{}", new Datetime(), msg, delayTime);
        sender.sendDeadMsg(msg, delayTime);
    }

    @RequestMapping("delayMsg")
    public void delayMsg(String msg, Integer delayTime) {
        log.info("当前时间：{},收到请求，msg:{},delayTime:{}", new Datetime(), msg, delayTime);
        sender.sendDelayMsg(msg, delayTime);
    }

}