#include "kafka_producer.h"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

class BufferWriter {
public:
    std::vector<uint8_t> buffer;
    
    void writeInt8(int8_t v) { buffer.push_back(v); }
    void writeUInt8(uint8_t v) { buffer.push_back(v); }
    
    void writeInt16BE(int16_t v) {
        buffer.push_back((v >> 8) & 0xFF);
        buffer.push_back(v & 0xFF);
    }
    
    void writeInt32BE(int32_t v) {
        buffer.push_back((v >> 24) & 0xFF);
        buffer.push_back((v >> 16) & 0xFF);
        buffer.push_back((v >> 8) & 0xFF);
        buffer.push_back(v & 0xFF);
    }
    
    void writeNullableString(const std::string* str) {
        if (!str) {
            writeInt16BE(-1);
        } else {
            writeInt16BE(str->length());
            for (char c : *str) buffer.push_back(c);
        }
    }
    
    void writeUVarInt(uint32_t value) {
        while (value >= 0x80) {
            buffer.push_back((value & 0x7f) | 0x80);
            value >>= 7;
        }
        buffer.push_back(value);
    }
    
    void writeCompactString(const std::string& str) {
        writeUVarInt(str.length() + 1);
        for (char c : str) buffer.push_back(c);
    }
    
    void writeBytes(const std::vector<uint8_t>& bytes) {
        for (uint8_t b : bytes) buffer.push_back(b);
    }
};

KafkaProducer::KafkaProducer(const std::string& host, int port, const std::string& topic)
    : host_(host), port_(port), topic_(topic), sock_(-1), correlation_id_(1), running_(false) {}

KafkaProducer::~KafkaProducer() {
    stop();
}

void KafkaProducer::start() {
    running_ = true;
    worker_ = std::thread(&KafkaProducer::background_loop, this);
}

void KafkaProducer::stop() {
    running_ = false;
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
    if (sock_ != -1) {
        close(sock_);
        sock_ = -1;
    }
}

void KafkaProducer::produce_trade(const Trade& trade) {
    std::lock_guard<std::mutex> lock(mu_);
    queue_.push_back(trade);
    cv_.notify_one();
}

bool KafkaProducer::connect_broker() {
    if (sock_ != -1) close(sock_);
    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port_);
    inet_pton(AF_INET, host_.c_str(), &serv_addr.sin_addr);

    if (connect(sock_, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        sock_ = -1;
        return false;
    }
    return true;
}

void KafkaProducer::send_produce_request(const std::vector<Trade>& batch) {
    if (sock_ == -1) return;

    // Serialize batch to JSON-like string lines
    std::string records_str = "";
    for (const auto& t : batch) {
        records_str += std::to_string(t.maker_order_id) + "," + 
                       std::to_string(t.taker_order_id) + "," + 
                       std::to_string(t.price) + "," + 
                       std::to_string(t.quantity) + "\n";
    }

    BufferWriter writer;
    std::string client_id = "OrderFlow";
    
    // Header v1
    writer.writeInt16BE(0); // api_key (Produce)
    writer.writeInt16BE(11); // api_version
    writer.writeInt32BE(correlation_id_++);
    writer.writeNullableString(&client_id);
    writer.writeUInt8(0); // TAG_BUFFER
    
    // Body (Produce v11)
    writer.writeCompactString(""); // transactional_id
    writer.writeInt16BE(1); // acks
    writer.writeInt32BE(1000); // timeout_ms
    
    writer.writeUVarInt(2); // topics array length (1 + 1)
    writer.writeCompactString(topic_);
    
    writer.writeUVarInt(2); // partitions array length (1 + 1)
    writer.writeInt32BE(0); // partition_index 0
    
    writer.writeUVarInt(records_str.length() + 1); // records_length
    for (char c : records_str) {
        writer.writeUInt8(c);
    }
    
    writer.writeUInt8(0); // partition TAG_BUFFER
    writer.writeUInt8(0); // topic TAG_BUFFER
    
    // Total message
    int32_t message_size = writer.buffer.size();
    BufferWriter final_writer;
    final_writer.writeInt32BE(message_size);
    final_writer.writeBytes(writer.buffer);
    
    // Send over socket
    send(sock_, final_writer.buffer.data(), final_writer.buffer.size(), 0);
    
    // Wait for response
    uint32_t resp_size_n;
    if (recv(sock_, &resp_size_n, 4, 0) == 4) {
        uint32_t resp_size = ntohl(resp_size_n);
        std::vector<uint8_t> resp_buf(resp_size);
        int total_received = 0;
        while (total_received < resp_size) {
            int n = recv(sock_, resp_buf.data() + total_received, resp_size - total_received, 0);
            if (n <= 0) break;
            total_received += n;
        }
    }
}

void KafkaProducer::background_loop() {
    while (running_) {
        std::vector<Trade> batch;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, std::chrono::milliseconds(50), [this] { 
                return !queue_.empty() || !running_; 
            });
            if (!running_ && queue_.empty()) break;
            
            batch = std::move(queue_);
            queue_.clear();
        }

        if (batch.empty()) continue;

        if (sock_ == -1) {
            if (!connect_broker()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                // Add back to queue on fail
                std::lock_guard<std::mutex> qlock(mu_);
                queue_.insert(queue_.begin(), batch.begin(), batch.end());
                continue;
            }
        }
        
        send_produce_request(batch);
    }
}