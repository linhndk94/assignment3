const amqplib = require('amqplib');
const fs = require('fs');
const path = require('path');
const Sharp = require('sharp')

const amqp_url = "amqp://assignment3:assignment3@54.237.122.168:5672";
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const bucket = "assignment3-user2386042";

async function consume() {
    const conn = await amqplib.connect(amqp_url, "heartbeat=60");
    const ch = await conn.createChannel()
    const q = 'assignment3_image_queue';
    await ch.consume(q, async function (msg) {
        const fileKey = msg.content.toString();
        console.log(fileKey);
        ch.ack(msg);
        const input = {
            "Bucket": bucket,
            "Key": fileKey
        }
        const fileName = extractFileName(fileKey);
        let readStream = s3.getObject(input).createReadStream();
        let writeStream = fs.createWriteStream(path.join(__dirname, fileName));
        readStream.pipe(writeStream);
        resize(fileName);
    }, {consumerTag: 'image_consumer'});
}

function extractFileName(fileKey) {
    console.log(fileKey);
    const split = fileKey.split("/");
    return split[split.length - 1];
}

function resize(fileName) {
    console.log(fileName);
    const readStream = fs.createReadStream(path.join(__dirname, fileName));
    let transform = Sharp();
    transform.toFormat("png");
    transform.resize(16, 16);
    readStream.pipe(transform);
    let writeStream = fs.createWriteStream(path.join(__dirname, "p" + fileName));
    transform.pipe(writeStream);
}

consume();