#pragma once

#include "tools/json.hpp"
#include "gen_string.hpp"
#include <atomic>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <iostream>
#include <lgraph/lgraph_rpc_client.h>

using namespace lgraph;


inline int run_scan(size_t vertex_num, std::vector<std::string>& config, int threads_num, int sub_property_id = 0) {
    std::function<void(RpcClient&, int64_t)> F;
    std::atomic<int64_t> sum = 0;

    F = [&](RpcClient& thread_client, int64_t src){
        std::string cypher = gen_cypher_get_all_edge_property(src, sub_property_id);
        std::string result;
        bool success = thread_client.CallCypher(result, cypher);
        if (!success) {
            std::cerr<< config[1] << "\n";
            std::cerr<< config[2] << "\n";
            std::cerr<< config[0] << "\n";
            std::cerr<< cypher << "\n";
            std::cerr<< result << "\n";
            assert(success);
        }
        nlohmann::json json = nlohmann::json::parse(result);
        if (json.empty()) {
            return ;
        }

        for (const auto& item : json) {
            int64_t weight = 0;
            std::string f_value = item["rf"];  // 根据实际返回字段名调整
            auto [ptr, ec] = std::from_chars(f_value.data(), f_value.data()+f_value.size(), weight);
            // std::cout << "Edge property f1: " << f1_value << std::endl;
            sum.fetch_add(weight);
        }
    };

    #pragma omp parallel num_threads(threads_num)
    {
        RpcClient thread_client(config[0], config[1], config[2]);
        #pragma omp for
        for (size_t v_i = 0; v_i < vertex_num; v_i++) {
            F(thread_client, v_i);
        }
    }
    std::cout << "scan sum=" << sum << std::endl;
  return 0;
}