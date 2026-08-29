import net from "net";
import fs from "fs";

let logDir = "/tmp/kraft-combined-logs";
if (process.argv.length > 2) {
    const propsPath = process.argv[2];
    try {
        const props = fs.readFileSync(propsPath, 'utf8');
        const match = props.match(/^log\.dirs=(.*)$/m);
        if (match) {
            logDir = match[1].trim();
        }
    } catch (e) {
        // ignore
    }
}

const logPath = `${logDir}/__cluster_metadata-0/00000000000000000000.log`;

function findTopicId(buffer, topicName) {
    if (!buffer) return null;
    
    // TopicRecord has type 2, version 0 -> 0x02, 0x00
    // Then topicName as compact string (length + 1)
    const nameLen = topicName.length + 1;
    let nameLenBytes = [];
    let value = nameLen;
    while (value >= 0x80) {
        nameLenBytes.push((value & 0x7f) | 0x80);
        value >>>= 7;
    }
    nameLenBytes.push(value);
    
    const prefix = Buffer.from([0x02, 0x00, ...nameLenBytes]);
    const topicBuffer = Buffer.from(topicName, 'utf8');
    const searchPattern = Buffer.concat([prefix, topicBuffer]);
    
    const idx = buffer.indexOf(searchPattern);
    if (idx !== -1) {
        const uuidOffset = idx + searchPattern.length;
        if (uuidOffset + 16 <= buffer.length) {
            return buffer.subarray(uuidOffset, uuidOffset + 16);
        }
    }
    return null;
}

function findPartitions(buffer, topicId) {
    if (!buffer || !topicId) return [];
    let offset = 0;
    const partitions = [];
    while (true) {
        let idx = buffer.indexOf(topicId, offset);
        if (idx === -1) break;
        
        if (idx >= 6 && buffer[idx - 6] === 0x03 && (buffer[idx - 5] === 0x00 || buffer[idx - 5] === 0x01)) {
            const partitionId = buffer.readInt32BE(idx - 4);
            let cursor = idx + 16;
            
            function readUVarInt() {
                let val = 0;
                let shift = 0;
                while (true) {
                    const byte = buffer[cursor++];
                    val |= (byte & 0x7f) << shift;
                    if ((byte & 0x80) === 0) break;
                    shift += 7;
                }
                return val;
            }
            
            function readCompactArray() {
                const len = readUVarInt() - 1;
                if (len < 0) return [];
                const arr = [];
                for (let i = 0; i < len; i++) {
                    arr.push(buffer.readInt32BE(cursor));
                    cursor += 4;
                }
                return arr;
            }
            
            const replicas = readCompactArray();
            const isr = readCompactArray();
            const removingReplicas = readCompactArray();
            const addingReplicas = readCompactArray();
            
            const leader = buffer.readInt32BE(cursor); cursor += 4;
            const leaderEpoch = buffer.readInt32BE(cursor); cursor += 4;
            
            partitions.push({
                partitionId,
                replicas,
                isr,
                leader,
                leaderEpoch
            });
        }
        offset = idx + 1;
    }
    return partitions;
}

function findTopicNameById(buffer, topicId) {
    if (!buffer || !topicId) return null;
    let offset = 0;
    while (true) {
        let idx = buffer.indexOf(Buffer.from([0x02, 0x00]), offset);
        if (idx === -1) break;
        
        let cursor = idx + 2;
        let nameLen = 0;
        let shift = 0;
        let valid = true;
        while (true) {
            if (cursor >= buffer.length) { valid = false; break; }
            const byte = buffer[cursor++];
            nameLen |= (byte & 0x7f) << shift;
            if ((byte & 0x80) === 0) break;
            shift += 7;
        }
        
        if (valid) {
            nameLen -= 1; // COMPACT_STRING length is N + 1
            if (nameLen > 0 && cursor + nameLen + 16 <= buffer.length) {
                const topicName = buffer.toString('utf8', cursor, cursor + nameLen);
                cursor += nameLen;
                const uuid = buffer.subarray(cursor, cursor + 16);
                if (uuid.equals(topicId)) {
                    return topicName;
                }
            }
        }
        offset = idx + 1;
    }
    return null;
}

class BufferReader {
    constructor(buffer) {
        this.buffer = buffer;
        this.offset = 0;
    }
    readInt8() { const v = this.buffer.readInt8(this.offset); this.offset += 1; return v; }
    readInt16BE() { const v = this.buffer.readInt16BE(this.offset); this.offset += 2; return v; }
    readInt32BE() { const v = this.buffer.readInt32BE(this.offset); this.offset += 4; return v; }
    readNullableString() {
        const len = this.readInt16BE();
        if (len === -1) return null;
        const str = this.buffer.toString('utf8', this.offset, this.offset + len);
        this.offset += len;
        return str;
    }
    readUVarInt() {
        let value = 0;
        let shift = 0;
        while (true) {
            const byte = this.buffer.readUInt8(this.offset++);
            value |= (byte & 0x7f) << shift;
            if ((byte & 0x80) === 0) break;
            shift += 7;
        }
        return value;
    }
    readCompactString() {
        const len = this.readUVarInt() - 1;
        if (len === -1) return null;
        const str = this.buffer.toString('utf8', this.offset, this.offset + len);
        this.offset += len;
        return str;
    }
    readCompactArrayLength() {
        return this.readUVarInt() - 1;
    }
}

class BufferWriter {
    constructor(size = 1024) {
        this.buffer = Buffer.alloc(size);
        this.offset = 0;
    }
    writeInt8(v) { this.buffer.writeInt8(v, this.offset); this.offset += 1; }
    writeUInt8(v) { this.buffer.writeUInt8(v, this.offset); this.offset += 1; }
    writeInt16BE(v) { this.buffer.writeInt16BE(v, this.offset); this.offset += 2; }
    writeInt32BE(v) { this.buffer.writeInt32BE(v, this.offset); this.offset += 4; }
    writeBigInt64BE(v) { this.buffer.writeBigInt64BE(v, this.offset); this.offset += 8; }
    writeUVarInt(value) {
        while (value >= 0x80) {
            this.buffer.writeUInt8((value & 0x7f) | 0x80, this.offset++);
            value >>>= 7;
        }
        this.buffer.writeUInt8(value, this.offset++);
    }
    writeCompactString(str) {
        this.writeUVarInt(str.length + 1);
        this.buffer.write(str, this.offset, str.length, 'utf8');
        this.offset += str.length;
    }
    writeBuffer(buf) {
        buf.copy(this.buffer, this.offset);
        this.offset += buf.length;
    }
    getBuffer() {
        return this.buffer.subarray(0, this.offset);
    }
}

const server = net.createServer((connection) => {
    let buffer = Buffer.alloc(0);

    connection.on("data", (data) => {
        buffer = Buffer.concat([buffer, data]);

        while (buffer.length >= 4) {
            const requestMessageSize = buffer.readInt32BE(0);
            const totalSize = 4 + requestMessageSize;

            if (buffer.length < totalSize) {
                break;
            }

            const requestData = buffer.subarray(0, totalSize);
            buffer = buffer.subarray(totalSize);

            const reader = new BufferReader(requestData);
            reader.offset = 4; // Skip message size

            const requestApiKey = reader.readInt16BE();
            const requestApiVersion = reader.readInt16BE();
            const correlationId = reader.readInt32BE();

            if (requestApiKey === 18) {
                // ApiVersions
                let errorCode = 0;
                if (requestApiVersion < 0 || requestApiVersion > 4) {
                    errorCode = 35; // UNSUPPORTED_VERSION
                }

                const writer = new BufferWriter();
                writer.writeInt32BE(correlationId);
                writer.writeInt16BE(errorCode);
                
                // api_keys array
                writer.writeUInt8(5); // length: 4 elements + 1
                
                // element 1: ApiVersions
                writer.writeInt16BE(18); // api_key
                writer.writeInt16BE(0); // min_version
                writer.writeInt16BE(4); // max_version
                writer.writeUInt8(0); // TAG_BUFFER

                // element 2: DescribeTopicPartitions
                writer.writeInt16BE(75); // api_key
                writer.writeInt16BE(0); // min_version
                writer.writeInt16BE(0); // max_version
                writer.writeUInt8(0); // TAG_BUFFER

                // element 3: Fetch
                writer.writeInt16BE(1); // api_key
                writer.writeInt16BE(0); // min_version
                writer.writeInt16BE(16); // max_version
                writer.writeUInt8(0); // TAG_BUFFER

                // element 4: Produce
                writer.writeInt16BE(0); // api_key
                writer.writeInt16BE(0); // min_version
                writer.writeInt16BE(11); // max_version
                writer.writeUInt8(0); // TAG_BUFFER
                
                writer.writeInt32BE(0); // throttle_time_ms
                writer.writeUInt8(0); // TAG_BUFFER

                const responseBody = writer.getBuffer();
                const response = Buffer.alloc(4 + responseBody.length);
                response.writeInt32BE(responseBody.length, 0);
                responseBody.copy(response, 4);
                connection.write(response);
            } else if (requestApiKey === 0) {
                // Produce
                const clientId = reader.readNullableString();
                reader.readUVarInt(); // header TAG_BUFFER
                
                const transactionalId = reader.readCompactString();
                const acks = reader.readInt16BE();
                const timeoutMs = reader.readInt32BE();
                
                const topicsCount = reader.readCompactArrayLength();
                let topicRequests = [];
                
                for (let t = 0; t < topicsCount; t++) {
                    const topicName = reader.readCompactString();
                    const partitionsCount = reader.readCompactArrayLength();
                    let partitionsRequests = [];
                    for (let i = 0; i < partitionsCount; i++) {
                        const partitionIndex = reader.readInt32BE();
                        const recordsLen = reader.readUVarInt() - 1;
                        let recordsBytes = Buffer.alloc(0);
                        if (recordsLen > 0) {
                            recordsBytes = reader.buffer.subarray(reader.offset, reader.offset + recordsLen);
                            reader.offset += recordsLen;
                        }
                        reader.readUVarInt(); // partition TAG_BUFFER
                        partitionsRequests.push({ index: partitionIndex, recordsBytes });
                    }
                    reader.readUVarInt(); // topic TAG_BUFFER
                    topicRequests.push({ topicName, partitionsRequests });
                }
                
                let clusterMetadata = null;
                try {
                    clusterMetadata = fs.readFileSync(logPath);
                } catch (e) {
                }
                
                const writer = new BufferWriter(1024);
                // Header v1
                writer.writeInt32BE(correlationId);
                writer.writeUInt8(0); // TAG_BUFFER
                
                // Body (Produce Response v11)
                writer.writeUInt8(topicRequests.length + 1); // responses array length
                
                for (const topicReq of topicRequests) {
                    writer.writeCompactString(topicReq.topicName);
                    
                    writer.writeUInt8(topicReq.partitionsRequests.length + 1); // partitions array length
                    
                    const topicId = findTopicId(clusterMetadata, topicReq.topicName);
                    let validPartitions = [];
                    if (topicId) {
                        validPartitions = findPartitions(clusterMetadata, topicId);
                    }
                    
                    for (const req of topicReq.partitionsRequests) {
                        let topicErrorCode = 3; // UNKNOWN_TOPIC_OR_PARTITION
                        
                        if (topicId) {
                            const partitionExists = validPartitions.some(p => p.partitionId === req.index);
                            if (partitionExists) {
                                topicErrorCode = 0; // NO_ERROR
                                
                                if (req.recordsBytes.length > 0) {
                                    const partitionDir = `${logDir}/${topicReq.topicName}-${req.index}`;
                                    fs.mkdirSync(partitionDir, { recursive: true });
                                    const partitionLogPath = `${partitionDir}/00000000000000000000.log`;
                                    const fd = fs.openSync(partitionLogPath, 'a');
                                    fs.appendFileSync(fd, req.recordsBytes);
                                    if (process.env.FSYNC_MODE === '1') {
                                        fs.fsyncSync(fd);
                                    }
                                    fs.closeSync(fd);
                                }
                            }
                        }
                        
                        const baseOffset = topicErrorCode === 0 ? 0n : -1n;
                        const logStartOffset = topicErrorCode === 0 ? 0n : -1n;
                        
                        writer.writeInt32BE(req.index);
                        writer.writeInt16BE(topicErrorCode); // error_code
                        writer.writeBigInt64BE(baseOffset); // base_offset
                        writer.writeBigInt64BE(-1n); // log_append_time_ms
                        writer.writeBigInt64BE(logStartOffset); // log_start_offset
                        
                        writer.writeUInt8(1); // record_errors (empty array)
                        writer.writeUInt8(0); // error_message (null string)
                        writer.writeUInt8(0); // partition TAG_BUFFER
                    }
                    
                    writer.writeUInt8(0); // topic TAG_BUFFER
                }
                
                writer.writeInt32BE(0); // throttle_time_ms
                writer.writeUInt8(0); // body TAG_BUFFER
                
                const responseBody = writer.getBuffer();
                const response = Buffer.alloc(4 + responseBody.length);
                response.writeInt32BE(responseBody.length, 0);
                responseBody.copy(response, 4);
                connection.write(response);
            } else if (requestApiKey === 75) {
                // DescribeTopicPartitions
                const clientId = reader.readNullableString();
                reader.readUVarInt(); // header TAG_BUFFER

                const topicCount = reader.readCompactArrayLength();
                let topicNames = [];
                for (let i = 0; i < topicCount; i++) {
                    topicNames.push(reader.readCompactString());
                    reader.readUVarInt(); // topic TAG_BUFFER
                }
                
                // Sort topics alphabetically as required by this stage
                topicNames.sort();

                const writer = new BufferWriter();
                // Header v1
                writer.writeInt32BE(correlationId);
                writer.writeUInt8(0); // TAG_BUFFER
                
                // Body
                writer.writeInt32BE(0); // throttle_time_ms
                writer.writeUInt8(topicNames.length + 1); // topics array length
                
                let clusterMetadata = null;
                try {
                    clusterMetadata = fs.readFileSync(logPath);
                } catch (e) {
                    // Ignore missing log file
                }

                for (const topicName of topicNames) {
                    const topicIdBuffer = findTopicId(clusterMetadata, topicName);
                    let partitions = [];
                    if (topicIdBuffer) {
                        partitions = findPartitions(clusterMetadata, topicIdBuffer);
                    }
                    
                    if (topicIdBuffer && partitions.length > 0) {
                        
                        writer.writeInt16BE(0); // error_code (0)
                        writer.writeCompactString(topicName);
                        writer.writeBuffer(topicIdBuffer);
                        writer.writeUInt8(0); // is_internal (false)
                        
                        // partitions array
                        writer.writeUInt8(partitions.length + 1); // length (N elements + 1)
                        
                        for (const partitionInfo of partitions) {
                            writer.writeInt16BE(0); // error_code
                            writer.writeInt32BE(partitionInfo.partitionId);
                            writer.writeInt32BE(partitionInfo.leader);
                            writer.writeInt32BE(partitionInfo.leaderEpoch);
                            
                            writer.writeUInt8(partitionInfo.replicas.length + 1);
                            for (const r of partitionInfo.replicas) writer.writeInt32BE(r);
                            
                            writer.writeUInt8(partitionInfo.isr.length + 1);
                            for (const r of partitionInfo.isr) writer.writeInt32BE(r);
                            
                            writer.writeUInt8(1); // eligible_leader_replicas
                            writer.writeUInt8(1); // last_known_elr
                            writer.writeUInt8(1); // offline_replicas
                            writer.writeUInt8(0); // partition TAG_BUFFER
                        }
                        
                    } else {
                        writer.writeInt16BE(3); // error_code (UNKNOWN_TOPIC_OR_PARTITION)
                        writer.writeCompactString(topicName);
                        
                        // topic_id: 16 bytes of 0s
                        writer.writeBuffer(Buffer.alloc(16, 0));
                        
                        writer.writeUInt8(0); // is_internal (false)
                        writer.writeUInt8(1); // partitions array length (0 elements + 1)
                    }

                    writer.writeInt32BE(0); // topic_authorized_operations
                    writer.writeUInt8(0); // topic TAG_BUFFER
                }
                
                writer.writeInt8(-1); // next_cursor (null)
                writer.writeUInt8(0); // TAG_BUFFER

                const responseBody = writer.getBuffer();
                const response = Buffer.alloc(4 + responseBody.length);
                response.writeInt32BE(responseBody.length, 0);
                responseBody.copy(response, 4);
                connection.write(response);
            } else if (requestApiKey === 1) {
                // Fetch
                const clientId = reader.readNullableString();
                reader.readUVarInt(); // header TAG_BUFFER
                
                // Fetch Request v16 Body
                reader.readInt32BE(); // max_wait_ms
                reader.readInt32BE(); // min_bytes
                reader.readInt32BE(); // max_bytes
                reader.readInt8();    // isolation_level
                reader.readInt32BE(); // session_id
                reader.readInt32BE(); // session_epoch
                
                const topicsCount = reader.readCompactArrayLength();
                let requestTopicId = Buffer.alloc(16, 0);
                let requestPartitionIndex = 0;
                if (topicsCount > 0) {
                    requestTopicId = Buffer.from(reader.buffer.subarray(reader.offset, reader.offset + 16));
                    reader.offset += 16;
                    
                    const partitionsCount = reader.readCompactArrayLength();
                    if (partitionsCount > 0) {
                        requestPartitionIndex = reader.readInt32BE(); // partition
                    }
                }

                const writer = new BufferWriter();
                // Header v1
                writer.writeInt32BE(correlationId);
                writer.writeUInt8(0); // TAG_BUFFER
                
                // Body
                writer.writeInt32BE(0); // throttle_time_ms
                writer.writeInt16BE(0); // error_code
                writer.writeInt32BE(0); // session_id
                
                let clusterMetadata = null;
                try {
                    clusterMetadata = fs.readFileSync(logPath);
                } catch (e) {
                    // Ignore missing log file
                }
                
                let topicErrorCode = 100; // UNKNOWN_TOPIC_ID
                let partitionLogBytes = Buffer.alloc(0);

                if (clusterMetadata && requestTopicId) {
                    const topicName = findTopicNameById(clusterMetadata, requestTopicId);
                    if (topicName) {
                        topicErrorCode = 0; // NO_ERROR
                        // Attempt to read partition log
                        const partitionLogPath = `${logDir}/${topicName}-${requestPartitionIndex}/00000000000000000000.log`;
                        try {
                            partitionLogBytes = fs.readFileSync(partitionLogPath);
                        } catch (e) {
                            // File not found, partition log bytes remain empty
                        }
                    }
                }

                if (topicsCount > 0) {
                    writer.writeUInt8(2); // responses array length (1 element + 1)
                    writer.writeBuffer(requestTopicId); // topic_id
                    
                    writer.writeUInt8(2); // partitions array length (1 element + 1)
                    writer.writeInt32BE(requestPartitionIndex); // partition_index
                    writer.writeInt16BE(topicErrorCode); // error_code
                    writer.writeInt32BE(0); writer.writeInt32BE(0); // high_watermark
                    writer.writeInt32BE(0); writer.writeInt32BE(0); // last_stable_offset
                    writer.writeInt32BE(0); writer.writeInt32BE(0); // log_start_offset
                    writer.writeUInt8(1); // aborted_transactions (empty array)
                    writer.writeInt32BE(0); // preferred_read_replica
                    
                    // records (COMPACT_RECORDS)
                    if (partitionLogBytes.length > 0) {
                        writer.writeUVarInt(partitionLogBytes.length + 1);
                        writer.writeBuffer(partitionLogBytes);
                    } else {
                        writer.writeUInt8(0); // null
                    }
                    
                    writer.writeUInt8(0); // partition TAG_BUFFER
                    
                    writer.writeUInt8(0); // topic TAG_BUFFER
                } else {
                    writer.writeUInt8(1); // responses array length (0 elements + 1)
                }
                
                writer.writeUInt8(0); // body TAG_BUFFER
                
                const responseBody = writer.getBuffer();
                const response = Buffer.alloc(4 + responseBody.length);
                response.writeInt32BE(responseBody.length, 0);
                responseBody.copy(response, 4);
                connection.write(response);
            }
        }
    });
});

server.listen(9092, "127.0.0.1");
