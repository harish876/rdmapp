#include "rdma_logger.h"

#include <algorithm>
#include <cctype>
#include <iterator>

namespace RDMA_EC {

std::atomic<bool> Logger::enabled_{true};
std::atomic<int> Logger::level_{
		static_cast<int>(Logger::Level::Info)};

bool Logger::should_log(Level message_level) {
	if (message_level == Level::Error) {
		return true;
	}

	if (!enabled_.load()) {
		return false;
	}

	return level_.load() >= static_cast<int>(message_level);
}

Logger::Level Logger::level_from_string(const std::string &level) {
	std::string normalized;
	normalized.reserve(level.size());
	std::transform(level.begin(), level.end(), std::back_inserter(normalized),
								 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

	if (normalized == "debug") {
		return Level::Debug;
	}
	if (normalized == "info") {
		return Level::Info;
	}
	if (normalized == "error") {
		return Level::Error;
	}

	// Default fallback
	return Level::Info;
}

const char *Logger::level_to_string(Level level) {
	switch (level) {
	case Level::Debug:
		return "debug";
	case Level::Info:
		return "info";
	case Level::Error:
	default:
		return "error";
	}
}

} // namespace RDMA_EC
