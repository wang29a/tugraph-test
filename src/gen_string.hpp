#pragma once

#include <cstdint>
#include <string>

inline std::string gen_cypher_get_edge_property(int64_t src, size_t idx = 0) {
    std::string ans;
    ans += "MATCH (a:Vertex {id: "
        + std::to_string(src)
        + "})-[r:Edge]->(b)";
    ans += "RETURN r.f" + std::to_string(idx+1) + "as rf";
    return ans;
}

inline std::string gen_cypher_get_dst(int64_t src) {
    std::string ans;
    ans += "MATCH (a:Vertex {id: "
        + std::to_string(src)
        + "})-[r:Edge]->(b)";
    ans += "RETURN b.id as dst";
    return ans;
}

inline std::string gen_cypher_get_dst(int64_t src, size_t idx) {
    std::string ans;
    ans += "MATCH (a:Vertex {id: "
        + std::to_string(src)
        + "})-[r:Edge]->(b)";
    ans += "RETURN b.id as dst r.f" + std::to_string(idx+1) + "as rf";
    return ans;
}

inline std::string gen_cypher_get_khop(int64_t src) {
    std::string ans;
    ans += "MATCH (a:Vertex {id: "
        + std::to_string(src)
        + "})-[r:Edge*2]->(b)";
    ans += "RETURN b.id as dst";
    return ans;
}