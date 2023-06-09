const amqplib = require('amqplib');
const fs = require('fs');
const path = require('path');
const Sharp = require('sharp')
const stream = require('stream');
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
        console.log("Create s3 Read Stream");
        let readStream = s3.getObject(input).createReadStream();
        // let writeStream = fs.createWriteStream(path.join(__dirname, fileName));
        // readStream.pipe(writeStream);
        // resize(fileName);
        console.log("Create Sharp");
        let transform = Sharp();
        transform.toFormat("png");
        transform.resize(16, 16);
        console.log("Transform");
        readStream.pipe(transform);
        console.log("Push to s3");
        transform.pipe(uploadFromStream(s3, {Bucket: bucket, Key: `processed/images/${fileName}`}));
    }, {consumerTag: 'image_consumer'});
}

function extractFileName(fileKey) {
    console.log("fileKey: " + fileKey);
    const split = fileKey.split("/");
    return split[split.length - 1];
}

function uploadFromStream(s3, {Bucket, Key}) {
    const pass = new stream.PassThrough();

    const params = {Bucket: Bucket, Key: Key, Body: pass};
    s3.upload(params, function(err, data) {
        console.log(err, data);
    });

    return pass;
}

consume();