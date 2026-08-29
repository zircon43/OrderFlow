const fs = require('fs');
const path = require('path');

const logDir = "/tmp/kraft-combined-logs";
const metadataDir = path.join(logDir, "__cluster_metadata-0");

fs.mkdirSync(metadataDir, { recursive: true });

const logPath = path.join(metadataDir, "00000000000000000000.log");

class BufferWriter {
    constructor(size = 1024) {
        this.buffer = Buffer.alloc(size);
        this.offset = 0;
    }
    writeUInt8(v) { this.buffer.writeUInt8(v, this.offset); this.offset += 1; }
    writeInt32BE(v) { this.buffer.writeInt32BE(v, this.offset); this.offset += 4; }
    writeUVarInt(value) {
        while (value >= 0x80) {
            this.buffer.writeUInt8((value & 0x7f) | 0x80, this.offset++);
            value >>>= 7;
        }
        this.buffer.writeUInt8(value, this.offset++);
    }
    writeBuffer(buf) {
        buf.copy(this.buffer, this.offset);
        this.offset += buf.length;
    }
    getBuffer() {
        return this.buffer.subarray(0, this.offset);
    }
}

const writer = new BufferWriter();
const topicName = "trades";
const topicId = Buffer.alloc(16, 1); // 16 bytes of 0x01 for UUID

// Write TopicRecord (Type 2, Version 0)
writer.writeUInt8(0x02);
writer.writeUInt8(0x00);
writer.writeUVarInt(topicName.length + 1);
writer.writeBuffer(Buffer.from(topicName, 'utf8'));
writer.writeBuffer(topicId);

// Write PartitionRecord (Type 3, Version 0)
writer.writeUInt8(0x03);
writer.writeUInt8(0x00);
writer.writeInt32BE(0); // partition 0
writer.writeBuffer(topicId);

// Compact Arrays for Replicas, ISR, Removing, Adding
writer.writeUVarInt(2); // length 1 (+1)
writer.writeInt32BE(1); // broker 1
writer.writeUVarInt(2); // length 1 (+1)
writer.writeInt32BE(1); // broker 1
writer.writeUVarInt(1); // length 0 (+1)
writer.writeUVarInt(1); // length 0 (+1)

// Leader and Epoch
writer.writeInt32BE(1); // leader
writer.writeInt32BE(0); // leader epoch

fs.writeFileSync(logPath, writer.getBuffer());
console.log(`Pre-provisioned topic '${topicName}' in ${logPath}`);
