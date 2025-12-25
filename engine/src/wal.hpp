#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <chrono>

// Simple Write-Ahead Log implementation
// Uses POSIX file I/O for appending records.
// Thread-unsafe (intended to be used by a single thread, e.g., Worker)

class WalLogger {
public:
    explicit WalLogger(const std::string& filepath) : filepath_(filepath) {
        // Open for appending, create if not exists
        // O_APPEND ensures atomic writes at the end of file in many filesystems
        fd_ = ::open(filepath_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            std::perror("Failed to open WAL file");
            throw std::runtime_error("Failed to open WAL file: " + filepath_);
        }
    }

    ~WalLogger() {
        if (fd_ >= 0) {
            Flush(); // Ensure data is flushed before closing
            ::close(fd_);
        }
    }

    // Append raw data to the log
    // Returns true on success
    bool Append(const void* data, size_t len) {
        // For higher throughput, we might want to buffer in userspace before writing syscall.
        // But let's start with direct write for simplicity and safety.
        // Or implement a simple buffer.
        
        if (buffer_pos_ + len > kBufferSize) {
            if (!Flush()) return false;
        }

        if (len > kBufferSize) {
            // Large write, bypass buffer
            return WriteRaw(data, len);
        }

        std::memcpy(buffer_ + buffer_pos_, data, len);
        buffer_pos_ += len;
        return true;
    }

    // Force flush to kernel buffers (write syscall)
    bool Flush() {
        if (buffer_pos_ == 0) return true;
        bool ok = WriteRaw(buffer_, buffer_pos_);
        buffer_pos_ = 0;
        return ok;
    }

    // Force flush to disk (fsync)
    // Warning: Expensive!
    void Fsync() {
        Flush();
        if (fd_ >= 0) {
            ::fsync(fd_);
        }
    }

    // Close explicitly
    void Close() {
        if (fd_ >= 0) {
            Flush();
            ::close(fd_);
            fd_ = -1;
        }
    }

    // Recover function: Read all records from the file and invoke callback
    template<typename RecordType, typename Callback>
    static void Recover(const std::string& filepath, Callback callback) {
        int fd = ::open(filepath.c_str(), O_RDONLY);
        if (fd < 0) {
            // File doesn't exist or cannot be opened, nothing to recover
            return;
        }

        RecordType record;
        ssize_t n;
        while ((n = ::read(fd, &record, sizeof(RecordType))) == sizeof(RecordType)) {
            callback(record);
        }

        ::close(fd);
    }

private:
    bool WriteRaw(const void* data, size_t len) {
        ssize_t written = ::write(fd_, data, len);
        if (written != (ssize_t)len) {
            std::perror("WAL Write failed");
            return false;
        }
        return true;
    }

    std::string filepath_;
    int fd_ = -1;

    // Userspace buffer to reduce syscalls
    static constexpr size_t kBufferSize = 4096 * 4; // 16KB
    char buffer_[kBufferSize];
    size_t buffer_pos_ = 0;
};

// Define the record format stored in WAL
struct WalRecord {
    uint64_t timestamp_ns;
    int64_t sku_id;
    int32_t qty;
    uint64_t request_id;
    uint64_t guest_id;
};
