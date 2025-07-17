#pragma once

#include <cstdint>
#include <string>

inline std::string gen_len_x_p(int64_t src, int64_t dst){ //长度为1 * 5 + 7 * 20 ，用|分隔 
    int dig = (src + dst) % 100000;
    std::string ans = std::to_string(dig);
    while(ans.size() < 5){
        ans = "0" + ans;
    }
    return ans;
}

inline std::string gen_cypher_add_node(int64_t src) {
    std::string ans;
    ans += "MERGE (v:Vertex {id: "
        + std::to_string(src)
        + "})";
    return ans;
}

inline std::string gen_cypher_add_edge(int64_t src, int64_t dst) {
    std::string ans;
    ans += "MATCH (from:Vertex {id: "
        + std::to_string(src)
        + "}), (to:Vertex {id: "
        + std::to_string(src)
        + "})";
    std::string p = gen_len_x_p(src, dst);
    std::string p4 = p+p+p+p;
    ans += "CREATE (from)-[r:Edge {f1: '" + p + "',"
        + "f2: '" + p + "',"
        + "f3: '" + p + "',"
        + "f4: '" + p + "',"
        + "f5: '" + p + "',"
        + "f6: '" + p + "',"
        + "f7: '" + p + "',"
        + "f8: '" + p + "'"
        + "}]->(to)";
    return ans;
}

inline std::string gen_cypher_get_all_edge_property(int64_t src, size_t idx = 0) {
    std::string ans;
    ans += "MATCH (a:Vertex {id: "
        + std::to_string(src)
        + "})-[r:Edge]->(b)";
    ans += "RETURN r.f" + std::to_string(idx+1) + " as rf";
    return ans;
}

inline std::string gen_cypher_get_edge_property(int64_t src, int64_t dst, size_t idx = 0) {
    std::string ans;
    ans += "MATCH (a:Vertex {id: "
        + std::to_string(src)
        + "})-[r:Edge]->(b:Vertex {id: "
        + std::to_string(dst) + "})";
    ans += "RETURN r.f"+std::to_string(idx+1);
    return ans;
}

inline std::string gen_cypher_update_edge(int64_t src, int64_t dst) {
    std::string ans;
    ans += "MATCH (a:Vertex {id: "
        + std::to_string(src)
        + "})-[r:Edge]->(b:Vertex {id: "
        + std::to_string(dst) + "})";
    std::string property = gen_len_x_p(src, dst);
    std::string property4;
    for (int i = 0; i < 5; i ++) {
        property4 += property;
    }
    ans += "SET r.f1 = \""+ property + "\","
        + "r.f2 = \""+ property4 + "\","
        + "r.f3 = \""+ property4 + "\","
        + "r.f4 = \""+ property4 + "\","
        + "r.f5 = \""+ property4 + "\","
        + "r.f6 = \""+ property4 + "\","
        + "r.f7 = \""+ property4 + "\","
        + "r.f8 = \""+ property4 + "\",";
    ans += "RETURN r";
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
    ans += "RETURN b.id as dst, r.f" + std::to_string(idx+1) + " as rf";
    return ans;
}

inline std::string gen_cypher_get_khop(int64_t src, int k) {
    std::string ans;
    ans += "MATCH (a:Vertex {id: "
        + std::to_string(src)
        + "})-[r:Edge*" + std::to_string(k) + "]->(b)";
    ans += "RETURN b.id as dst";
    return ans;
}