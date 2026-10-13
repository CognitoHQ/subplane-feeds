#include "recorder.h"

namespace feeds {

bool Recorder::open(const std::string& path, std::string& err) {
    f_ = std::fopen(path.c_str(), "w");
    if (!f_) { err = "cannot open " + path + " for writing"; return false; }
    return true;
}

void Recorder::frame(uint64_t ts, std::string_view json) noexcept {
    if (!f_) return;
    std::fprintf(f_, "{\"ts\":%llu,\"frame\":%.*s}\n", static_cast<unsigned long long>(ts), static_cast<int>(json.size()), json.data());
    ++lines_;
}

void Recorder::snapshot(uint64_t ts, std::string_view symbol, std::string_view body) noexcept {
    if (!f_) return;
    std::fprintf(f_, "{\"ts\":%llu,\"snapshot\":\"%.*s\",\"body\":%.*s}\n", static_cast<unsigned long long>(ts),
                 static_cast<int>(symbol.size()), symbol.data(), static_cast<int>(body.size()), body.data());
    ++lines_;
}

void Recorder::event(uint64_t ts, std::string_view name) noexcept {
    if (!f_) return;
    std::fprintf(f_, "{\"ts\":%llu,\"event\":\"%.*s\"}\n", static_cast<unsigned long long>(ts), static_cast<int>(name.size()), name.data());
    ++lines_;
}

void Recorder::close() noexcept {
    if (f_) { std::fclose(f_); f_ = nullptr; }
}

} // namespace feeds
