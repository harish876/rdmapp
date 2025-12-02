#pragma once

#include <rdmapp/detail/debug.h>
#include <atomic>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>

namespace RDMA_EC {

class Logger {
public:
    enum class Level { Error = 0, Info = 1, Debug = 2 };

    static void set_enabled(bool enabled) {
        enabled_.store(enabled);
    }
    
    static bool is_enabled() {
        return enabled_.load();
    }

    static void set_level(Level level) {
        level_.store(static_cast<int>(level));
    }

    static Level get_level() {
        return static_cast<Level>(level_.load());
    }

    static Level level_from_string(const std::string &level);
    static const char *level_to_string(Level level);
    
    class LogStream {
    public:
        LogStream(bool enabled, const char *prefix)
            : enabled_(enabled), prefix_(prefix) {}
        
        ~LogStream() {
            if (enabled_) {
                printf("%s %s\n", prefix_, ss_.str().c_str());
            }
        }
        
        template<typename T>
        LogStream& operator<<(const T& value) {
            if (enabled_) {
                ss_ << value;
            }
            return *this;
        }
        
        LogStream& operator<<(std::ostream& (*manip)(std::ostream&)) {
            if (enabled_) {
                ss_ << manip;
            }
            return *this;
        }
        
    private:
        bool enabled_;
        const char* prefix_;
        std::ostringstream ss_;
    };
    
    static LogStream info() {
        return LogStream(should_log(Level::Info), "[INFO]");
    }
    
    static LogStream debug() {
        return LogStream(should_log(Level::Debug), "[DEBUG]");
    }
    
    static LogStream error() {
        return LogStream(should_log(Level::Error), "[ERROR]");
    }

private:
    static bool should_log(Level message_level);

    static std::atomic<bool> enabled_;
    static std::atomic<int> level_;
};

} // namespace RDMA_EC

