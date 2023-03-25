const amqplib = require('amqplib');

const amqp_url = "amqp://assignment3:assignment3@54.237.122.168:5672";

const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

const region = "us-east-1";
const s3Client = new S3Client({region: region});
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
        const command = new GetObjectCommand(input);
        const response = await s3Client.send(command);
        let cache = [];
        console.log(JSON.stringify(response, (key, value) => {
            if (typeof value === 'object' && value !== null) {
                // Duplicate reference found, discard key
                if (cache.includes(value)) return;

                // Store value in our collection
                cache.push(value);
            }
            return value;
        }));
        cache = null; // Enable garbage collection
    }, {consumerTag: 'image_consumer'});
}

consume();