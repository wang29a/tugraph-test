//这个算法跟khop的区别就是添加了过滤功能，你会过滤掉那些边的权重小于bound的边

#pragma once

#include "lgraph/lgraph_rpc_client.h"
#include "gen_string.hpp"
#include <atomic>
#include <boost/mpl/aux_/na_fwd.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

using namespace lgraph;


inline int run_khop_plus(
    size_t vertex_num, std::vector<std::string> config,
    int threads_num, int k = 2, 
    int sub_property_id = 0,uint64_t bound = 0
) {
    std::atomic<int64_t> sum = 0;
    std::function<void(RpcClient, int64_t, int)> F;
    F = [&](RpcClient thread_client, int64_t src, int k){
        std::string cypher = gen_cypher_get_dst(src);
        std::string result;
        bool success = thread_client.CallCypher(result, cypher);
        nlohmann::json json = nlohmann::json::parse(result);
        auto data = json["data"];

        for (const auto& item : data) {
            int64_t dst = item["dst"];
            std::string weight = item["rf"];
            if (std::stoi(weight) < bound) {
                continue;
            }
            sum.fetch_add(dst);
            if (k > 0) {
                F(thread_client, dst, k-1);
            }
        }
    };
    int64_t nsrc = vertex_num / 10000; //从最大节点数中均匀取出10000个
    #pragma omp parallel num_threads(threads_num) //使用16个线程并行计算
    {
        static RpcClient thread_client(config[0], config[1], config[2]);
        #pragma omp for
        for (int64_t v_i = 0; v_i < vertex_num; v_i += nsrc) {
            F(thread_client, v_i, k-1);
        }
    }
    std::cout<<"khop plus sum="<<sum<<"\n";
    return 0;
}