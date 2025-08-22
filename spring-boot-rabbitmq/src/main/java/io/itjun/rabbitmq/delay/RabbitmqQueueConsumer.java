package io.itjun.rabbitmq.delay;

import static site.diteng.rabbitmq.DelayedRabbitMQConfig.DELAYED_QUEUE_NAME;
import static site.diteng.rabbitmq.RabbitMQConfig.DEAD_LETTER_QUEUEA_NAME;
import static site.diteng.rabbitmq.RabbitMQConfig.DEAD_LETTER_QUEUEB_NAME;
import static site.diteng.rabbitmq.RabbitMQConfig.DEAD_LETTER_QUEUEC_NAME;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cn.cerc.db.core.Datetime;

@Component
public class RabbitmqQueueConsumer {
    private static final Logger log = LoggerFactory.getLogger(RabbitmqQueueConsumer.class);

    @RabbitListener(queues = DEAD_LETTER_QUEUEA_NAME)
    public void receiveA(Message message, Channel channel) throws IOException {
        String msg = new String(message.getBody());
        log.info("当前时间：{},死信队列A收到消息：{}", new Datetime().toString(), msg);
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    @RabbitListener(queues = DEAD_LETTER_QUEUEB_NAME)
    public void receiveB(Message message, Channel channel) throws IOException {
        String msg = new String(message.getBody());
        log.info("当前时间：{},死信队列B收到消息：{}", new Datetime().toString(), msg);
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    @RabbitListener(queues = DEAD_LETTER_QUEUEC_NAME)
    public void receiveC(Message message, Channel channel) throws IOException {
        String msg = new String(message.getBody());
        log.info("当前时间：{},死信队列C收到消息：{}", new Datetime().toString(), msg);
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    @RabbitListener(queues = DELAYED_QUEUE_NAME)
    public void receiveD(Message message, Channel channel) throws IOException {
        String msg = new String(message.getBody());
        log.info("当前时间：{},延时队列收到消息：{}", new Datetime().toString(), msg);
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

}
