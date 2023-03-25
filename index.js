const amqplib = require('amqplib');

const amqp_url = "amqp://assignment3:assignment3@54.237.122.168:5672";

async function consume() {
    const conn = await amqplib.connect(amqp_url, "heartbeat=60");
    const ch = await conn.createChannel()
    const q = 'assignment3_image_queue';
    await ch.consume(q, function (msg) {
        console.log(msg.content.toString());
        ch.ack(msg);
        // ch.cancel('image_consumer');
    }, {consumerTag: 'image_consumer'});
}

consume();