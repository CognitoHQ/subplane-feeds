// The copied generated header must match the copied schema: every field offset in
// the table equals header size + the XML's declared offset for that message.
#include "canonical_fields.h"

#include "doctest.h"

#include <fstream>
#include <regex>
#include <sstream>

using namespace feeds;

TEST_CASE("canonical field table matches contract/subplane_canonical.xml") {
    std::ifstream in(std::string(FEEDS_SOURCE_DIR) + "/contract/subplane_canonical.xml");
    REQUIRE(in);
    std::stringstream ss; ss << in.rdbuf();
    const std::string xml = ss.str();
    const std::regex msg_re(R"(<sbe:message name=\"(\w+)\" id=\"(\d+)\" blockLength=\"(\d+)\")");
    const std::regex field_re(R"(<field name=\"(\w+)\"\s+id=\"\d+\"\s+type=\"\w+\"\s+offset=\"(\d+)\")");
    size_t messages = 0, fields = 0;
    for (auto it = std::sregex_iterator(xml.begin(), xml.end(), msg_re); it != std::sregex_iterator(); ++it) {
        const std::string name = (*it)[1];
        const CanonicalMessage* m = find_message(name);
        REQUIRE_MESSAGE(m != nullptr, name);
        CHECK(m->template_id == std::stoul((*it)[2]));
        CHECK(m->size == std::stoul((*it)[3]) + sp_canonical::kHeaderSize);
        ++messages;
        const size_t start = static_cast<size_t>(it->position());
        const size_t end = xml.find("</sbe:message>", start);
        const std::string body = xml.substr(start, end - start);
        for (auto f = std::sregex_iterator(body.begin(), body.end(), field_re); f != std::sregex_iterator(); ++f) {
            const std::string fname = (*f)[1];
            const unsigned long off = std::stoul((*f)[2]) + sp_canonical::kHeaderSize;
            if (fname == "InstrumentId") { CHECK(kPrefixInstrumentId == off); continue; }
            if (fname == "VenueId") { CHECK(kPrefixVenueId == off); continue; }
            if (fname == "Sequence") { CHECK(kPrefixSequence == off); continue; }
            if (fname == "RecvTs") { CHECK(kPrefixRecvTs == off); continue; }
            const CanonicalField* cf = find_field(*m, fname);
            REQUIRE_MESSAGE(cf != nullptr, (name + "." + fname));
            CHECK_MESSAGE(cf->offset == off, (name + "." + fname));
            ++fields;
        }
    }
    CHECK(messages == 8);
    CHECK(fields > 30);
}
