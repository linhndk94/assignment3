const amqplib = require('amqplib');
const fs = require('fs');
const path = require('path');

const amqp_url = "amqp://assignment3:assignment3@54.237.122.168:5672";
const AWS = require('aws-sdk');
const region = "us-east-1";
const s3 = new AWS.S3();
const bucket = "assignment3-user2386042";

async function consume() {
    const conn = await amqplib.connect(amqp_url, "heartbeat=60");
    const ch = await conn.createChannel()
    const q = 'assignment3_image_queue';
    await ch.consume(q, async function (msg) {
        console.log(msg.content.toString());
        ch.ack(msg);
        const input = {
            "Bucket": bucket,
            "Key": msg.content.toString()
        }
        let readStream = s3.getObject(input).createReadStream();
        let writeStream = fs.createWriteStream(path.join(__dirname, 'lorem.png'));
        readStream.pipe(writeStream);
    }, {consumerTag: 'image_consumer'});
}

consume();