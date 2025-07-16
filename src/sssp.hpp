
// void run_sssp(RpcClient& main_client, int64_t source) {
//     try {
//         // 1. 获取图中节点总数和最大节点ID
//         string node_info_result;
//         bool success = main_client.CallCypher(node_info_result, 
//             "MATCH (n:Vertex) RETURN count(n) AS count, max(n.id) AS max_id");
//         if (!success) {
//             cerr << "Failed to get node info: " << node_info_result << "\n";
//             return;
//         }
        
//         auto node_info_json = json::parse(node_info_result);
//         size_t node_count = node_info_json[0]["count"].get<size_t>();
//         size_t max_node_id = node_info_json[0]["max_id"].get<size_t>();
        
//         cout << "Node count: " << node_count << ", Max node ID: " << max_node_id << "\n";
        
//         // 使用最大节点ID+1作为graph_num，确保能容纳所有节点
//         size_t graph_num = max_node_id + 1;
        
//         // 2. 初始化数据结构
//         value_t DEFAULT_VALUE = numeric_limits<value_t>::max();
//         vector<value_t> values(graph_num, DEFAULT_VALUE);
        
//         unique_ptr<Bitmap> active_in, active_out;
//         try {
//             active_in = make_unique<Bitmap>(graph_num);
//             active_out = make_unique<Bitmap>(graph_num);
//             cout << "Allocated Bitmaps successfully" << "\n";
//         } catch (const exception& e) {
//             cerr << "Bitmap allocation failed: " << e.what() << "\n";
//             return;
//         }

//         active_in->clear();
//         active_in->set_bit(source);
//         values[source] = 0;

//         // 获取连接参数用于线程本地客户端
//         const string url = "127.0.0.1:9090";
//         const string user = "admin";
//         const string password = "73@TuGraph";

//         // 3. 定义处理函数
//         auto F = [&](size_t src) {
//             int32_t activated = 0;
//             string edges_result;
            
//             // 每个线程创建自己的RPC客户端
//            static thread_local RpcClient* thread_client = nullptr;
//             if (!thread_client) {
//                 thread_client = new RpcClient(url, user, password);
//                 cout << "Thread " << omp_get_thread_num() << " RPC client initialized" << "\n";
//             }
            
//             string cypher = "MATCH (s:Vertex {id: " + to_string(src) + "})-[e:Edge]->(d:Vertex) RETURN d.id AS dst, e.f1 AS weight";
            
//             bool success = thread_client->CallCypher(edges_result, cypher);
//             if (!success) {
//                 cerr << "Failed to get edges for node " << src << ": " << edges_result << "\n";
//                 return activated;
//             }
            
//             try {
//                 auto edges_json = json::parse(edges_result);
//                 if (!edges_json.is_array()) {
//                     cerr << "Invalid response format for node " << src << ": " << edges_result << "\n";
//                     return activated;
//                 }
                
//                 for (auto& edge : edges_json) {
//                     try {
//                         size_t dst = edge["dst"].get<size_t>();
//                         if (dst >= graph_num) {
//                             cerr << "Error: dst " << dst << " out of bounds (max=" << graph_num-1 << ")" << "\n";
//                             continue;
//                         }
                        
//                         value_t weight = stoll(edge["weight"].get<string>());
//                         value_t relax_dist = values[src] + weight;
                        
//                         if (relax_dist < values[dst]) {
//                             if (write_min(&values[dst], relax_dist)) {
//                                 #pragma omp critical
//                                 {
//                                     active_out->set_bit(dst);
//                                 }
//                                 activated++;
//                             }
//                         }
//                     } catch (const exception& e) {
//                         cerr << "Error processing edge for node " << src << ": " << e.what() << "\n";
//                         cerr << "Edge data: " << edge.dump() << "\n";
//                     }
//                 }
//             } catch (const exception& e) {
//                 cerr << "Error parsing response for node " << src << ": " << e.what() << "\n";
//                 cerr << "Response: " << edges_result << "\n";
//             }
//             return activated;
//         };

//         int step = 0;
//         int32_t activated = 1;
//         size_t basic_chunk = 64;
//         const int max_threads = 8;
        
//         omp_set_num_threads(max_threads);
        
//         while (activated > 0 && step < graph_num * 2) {
//             cout << "step=" << (++step) << " activated=" << activated << "\n";
            
//             activated = 0;
//             active_out->clear();
            
//             #pragma omp parallel for reduction(+:activated) schedule(dynamic, 1000)
//             for (size_t begin_v_i = 0; begin_v_i < graph_num; begin_v_i += basic_chunk) {
//                 size_t v_i = begin_v_i;
//                 unsigned long word = active_in->data[WORD_OFFSET(v_i)];
//                 while (word != 0) {
//                     if (word & 1) {
//                         if (v_i >= graph_num) {
//                             cerr << "Error: v_i " << v_i << " out of bounds (graph_num=" << graph_num << ")" << "\n";
//                             break;
//                         }
//                         activated += F(v_i);
//                     }
//                     v_i++;
//                     word = word >> 1;
//                 }
//             }
//             swap(active_in, active_out);
//         }

//         if (step >= graph_num * 2) {
//             cerr << "Warning: Exceeded maximum steps (" << graph_num * 2 << ")" << "\n";
//         }

//         // 5. 输出结果
//         value_t sum = 0;
//         size_t reachable_nodes = 0;
//         #pragma omp parallel for reduction(+:sum) reduction(+:reachable_nodes)
//         for (size_t i = 0; i < graph_num; i++) {
//             if (values[i] != DEFAULT_VALUE) {
//                 sum += values[i];
//                 reachable_nodes++;
//             }
//         }
        
//         cout << "\n=== SSSP Results ===" << "\n";
//         cout << "Total reachable nodes: " << reachable_nodes << "/" << node_count << "\n";
//         cout << "Total sum of shortest paths: " << sum << "\n";
//         cout << "Total steps: " << step << "\n";

//     } catch (const exception& e) {
//         cerr << "Error in run_sssp: " << e.what() << "\n";
//     }
// }

#pragma once

#include "lgraph/lgraph_rpc_client.h"
#include "src/atomic.hpp"
#include "src/gen_string.hpp"
#include "tools/json.hpp"
#include <atomic>
#include <charconv>
#include <cassert>
#include <cstddef>
#include <functional>
#include <limits>
#include <string>
#include <vector>
#include <iostream>
#include "bitmap.hpp"

using value_t = int64_t;

inline int run_sssp(
    size_t vertex_num, std::vector<std::string> config,
    int thread_num,
    int sub_property_id = 0, int64_t root_ = 0
) {
    value_t DEFAULT_VALUE = std::numeric_limits<value_t>::max();
    int64_t root = root_;
    
    std::cout << " source=" << root << std::endl;
    std::vector<value_t> values(vertex_num, DEFAULT_VALUE);//距离数组
    size_t basic_chunk = 64;
    Bitmap* active_out = new Bitmap(vertex_num);//访问数组
    Bitmap* active_in = new Bitmap(vertex_num);
    active_in->clear();
    active_in->set_bit(root);
    int32_t activated = 1;
    values[root] = 0;

    std::function<int32_t(lgraph::RpcClient&, int64_t)> F;
    F = [&](lgraph::RpcClient& thread_client, int64_t src){
            value_t src_value = values[src];
            int32_t activated = 0;

            std::string cypher = gen_cypher_get_dst(src, sub_property_id);
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
                // TODO
                return 0;
            }
            for (const auto& item : json) {
                int64_t dst = item["dst"];
                std::string payload = item["rf"];
                value_t weight = 0;
                auto [ptr, ec] = std::from_chars(payload.data(), payload.data() + payload.size(), weight);
                value_t relax_dist = values[src] + weight;
                if (relax_dist < values[dst]) {
                    if (write_min(&values[dst], relax_dist)) {
                        active_out->set_bit(dst);
                        activated++;
                    }
                }
            }
            return activated;
      };

    int step = 0;
    while (activated > 0) {
        std::cout << "step=" << (++step) << " activated=" << activated << std::endl;
        activated = 0;
        active_out->clear();
        assert(step <= vertex_num);
        #pragma omp parallel num_threads(thread_num) reduction(+:activated)
        {

            lgraph::RpcClient thread_client(config[0], config[1], config[2]);
            #pragma omp for
            for (int64_t begin_v_i = 0; begin_v_i < vertex_num; begin_v_i += basic_chunk) {
                int64_t v_i = begin_v_i;
                unsigned long word = active_in->data[WORD_OFFSET(v_i)]; 
                //这里是指，取出v_i开始的64个访问数组单位，一次for循环处理64个节点的访问
                while (word != 0) {
                    if (word & 1) {
                        activated += F(thread_client, v_i);
                    }
                    v_i++;
                    word = word >> 1;
                }
            }
        }
        std::swap(active_in, active_out);
    }

    std::atomic<value_t> sum{0};
    #pragma omp parallel for num_threads(thread_num)
    for (int64_t i = 0; i < vertex_num; i++) {
        if (values[i] != DEFAULT_VALUE) {
            sum.fetch_add(values[i]);
        }
    }
    std::cout << "scan sum=" << sum << std::endl;
    delete active_in;
    delete active_out;
  return 0;
}
