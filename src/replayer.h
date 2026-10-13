#pragma once

// Feeds a recording (see recorder.h) back through a Gateway, in file order,
// with the recorded receive timestamps, so output is deterministic.

#include <cstdint>
#include <string>

namespace feeds {

class Gateway;

struct ReplayStats { uint64_t frames = 0, snapshots = 0, events = 0, bad_lines = 0; };

bool replay_file(const std::string& path, Gateway& gw, ReplayStats& stats, std::string& err);

} // namespace feeds
