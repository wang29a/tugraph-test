// #include "bitmap.hpp"
// #include "atomic.hpp"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include <string>
#include <lgraph/lgraph_rpc_client.h>
#include "src/gen_string.hpp"
#include "src/khop.hpp"
#include "src/khop_plus.hpp"
#include "src/scan.hpp"
#include "src/sssp.hpp"
#include "tools/json.hpp"
#include <omp.h>
#include <random>
#include <unordered_set>
#include <sstream>
#include <atomic>

using namespace lgraph;
using json = nlohmann::json;
using value_t = int64_t;

thread_local std::mt19937 gen(std::random_device{}());
std::string generate_random_five_digit() {
    std::uniform_int_distribution<> dist(10000, 99999);
    return std::to_string(dist(gen));
}

int random_uniform_int(int min = 0, int max = 1) {
  unsigned seed = 2000; //2000
  static thread_local std::mt19937 generator(seed);
  std::uniform_int_distribution<int> distribution(min, max);
  return distribution(generator);
}


std::vector<std::pair<int64_t, int64_t>> edges;
std::unordered_set<int64_t> nodes;

// 辅助函数：拼接向量元素为字符串（用分隔符连接）
template <typename InputIt>
std::string join(InputIt first, InputIt last, const std::string& delimiter) {
    if (first == last) return "";
    std::ostringstream ss;
    copy(first, last, std::ostream_iterator<std::string>(ss, delimiter.c_str()));
    std::string result = ss.str();
    return result.substr(0, result.size() - delimiter.size()); // 移除最后一个分隔符
}

void ReadEdgesWithNodes(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open edge file: " + file_path);
    }
    
    int64_t src, dst;
    
    // int64_t mx=0; 
    
    while (file.read(reinterpret_cast<char*>(&src), sizeof(src)) &&
           file.read(reinterpret_cast<char*>(&dst), sizeof(dst))) {
        edges.emplace_back(src, dst);
        nodes.insert(src);
        nodes.insert(dst);
        // mx=max(mx,src);
        // mx=max(mx,dst);
    }
    
    // cout<<"最大点id："<<mx<<"\n";
    
    file.close();
    std::cout << "Read " << edges.size() << " edges and " << nodes.size() << " unique nodes." << "\n";
}

void ReadEdgesWithNodesTest(const std::string& file_path) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open edge file: " + file_path);
    }
    
    std::string line;
    while (getline(file, line)) {
        std::istringstream iss(line);
        int src, dst;
        if (iss >> src>>dst) {
            edges.emplace_back(src, dst);
            nodes.insert(src);
            nodes.insert(dst);
        } else {
            std::cerr<< "Warning: Invalid line format: " << line <<"\n";
        }
    }
    file.close();
    std::cout << "Read " << edges.size() << " edges and " << nodes.size() << " unique nodes." << "\n";
}


void run_app(std::vector<std::string>& config){
    /******************************** Test APP ********************************/
    std::string alg_app;
    int repeat_time = 1;
    std::vector<std::string> alg_app_set = {// 只要测试这四个
                                            "khop"
                                            ,"khop_plus"
                                            ,"scan"
                                            ,"sssp"
                                            };

    for (auto& alg_app : alg_app_set) {

      double all_run_time = 0;
    //   double before = utils::get_memory_usage();
      for (int i = 0; i < repeat_time; i++) {
        std::cout << "  alg_app: " << alg_app << "\n";
        std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
        size_t vertex_num = nodes.size();
        // double before = utils::get_memory_usage();
        if (alg_app == "sssp") {
          // for(int j = 0; j < graphdata.node_num; j += graphdata.node_num / 100){
          //   run_sssp(txn, seq, 0, j);
          // }
            run_sssp(vertex_num, config, 8);
        } else if (alg_app == "scan") {
            run_scan(vertex_num, config, 8);
        } else if (alg_app == "khop") {
            run_khop_recur(vertex_num, config, 8);
        } else if (alg_app == "khop_plus") {
            run_khop_plus(vertex_num, config, 8, 0, 0,50000);
        } else {
          std::cout << "No this type, alg_app_type=" << alg_app << "\n";
        }
        std::chrono::steady_clock::time_point t2 
            = std::chrono::steady_clock::now();
        std::chrono::duration<double> time_span 
            = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        all_run_time += time_span.count();
        // double over = utils::get_memory_usage();
        std::cout << " run_time=" << time_span.count() 
                //   << " run_mem=" << (over - before)
                  << "\n";
        // utils::get_io_info(alg_app + "_" + std::to_string(i) + "_");
      }

      std::cout << "Run time(sec): " << "\n";
      std::cout << "  @" << alg_app << ": " 
                << all_run_time / repeat_time
                << "\n";
    //   double over = utils::get_memory_usage();
    //   printf("  %s Memory usage(GB): %.3f\n", alg_app.c_str(), over - before);
    }
}

void insert_nodes(std::vector<std::string>& config, std::string data_path, int therad_num) {
    RpcClient client(config[0], config[1], config[2]);

    // 清除原有数据库
    std::string str;
    bool res = client.CallCypher(str, "CALL db.dropDB()");
    std::cout << "Drop database result: " << (res ? "success" : "failed") << "\n";

    // 创建顶点标签（添加RETURN语句以减少输出）
    res = client.CallCypher(str, 
        "CALL db.createVertexLabel('Vertex','id','id', int64, false) RETURN 'Label created'");
    std::cout << "Vertex label creation result: " << (res ? "success" : "failed") << "\n";

    // 创建边标签（添加RETURN语句以减少输出）
    // res = client.CallCypher(str, 
    //     "CALL db.createEdgeLabel('Edge', '[]', 'f1', STRING, true, 'f2', STRING, true, 'f3', STRING, true, 'f4', STRING, true) RETURN 'Edge label created'");
    res = client.CallCypher(str, 
        R"(CALL db.createEdgeLabel('Edge', '[]',
                    'f1', STRING, false,
                    'f2', STRING, false,
                    'f3', STRING, false,
                    'f4', STRING, false,
                    'f5', STRING, false,
                    'f6', STRING, false,
                    'f7', STRING, false,
                    'f8', STRING, false)
                    RETURN 'Edge label created')");
    std:: cout << "Edge label creation result: " << (res ? "success" : "failed") << "\n";
    ReadEdgesWithNodesTest(data_path);
    
    // ================== 并行批量导入顶点 ==================
    std::cout << "=== Starting to import nodes in parallel ===" << "\n";
    std::vector<int64_t> node_list(nodes.begin(), nodes.end());
    std::atomic<size_t> total_nodes(0);
    const size_t node_batch_size = 100000;
    const size_t node_report_interval = 100000;
    
    #pragma omp parallel num_threads(therad_num)
    {
        RpcClient thread_client(config[0], config[1], config[2]);
        
        // 使用静态调度，每个线程处理固定范围的顶点
        #pragma omp for schedule(static, node_batch_size)
        for (size_t i = 0; i < node_list.size(); i++) {
            size_t batch_start = (i / node_batch_size) * node_batch_size;
            size_t batch_end = std::min(batch_start + node_batch_size, node_list.size());
            
            // 确保每个线程只处理自己的批次
            if (i != batch_start) continue;
            
            std::string nodes_json = "[";
            for (size_t j = batch_start; j < batch_end; j++) {
                if (j > batch_start) nodes_json += ",";
                nodes_json += "{id:" + std::to_string(node_list[j]) + "}";
            }
            nodes_json += "]";
            
            std::string cypher = "CALL db.upsertVertex('Vertex', " + nodes_json + ")";
            std::string result;
            bool success = thread_client.CallCypher(result, cypher);
            
            #pragma omp critical
            {
                if (!success) {
                    std::cerr << "Failed to import nodes batch [" << batch_start << "-" << batch_end << "]: " << result << "\n";
                } else {
                    size_t batch_count = batch_end - batch_start;
                    total_nodes += batch_count;
                    if (batch_start % node_report_interval == 0) {
                        std::cout << "Imported " << batch_start << "/" << node_list.size() << " nodes" << "\n";
                    }
                }
            }
        }
    }
    std::cout << "Successfully imported " << total_nodes << " nodes in parallel." << "\n";
    // nodes.clear();
    std::string count_result;
    bool success = client.CallCypher(count_result, "MATCH (n:Vertex) RETURN count(n)");
    if (success) {
        auto node_count = json::parse(count_result)[0]["count(n)"].get<size_t>();
        std::cout << "实际数据库中的顶点数量: " << node_count << "\n";
    } else {
        std::cerr << "查询顶点数量失败: " << count_result << "\n";
    }
    // std::string result;
    // success = client.CallCypher(result, "CREATE INDEX ON :Vertex(id);");
    // if (success) {
    //     std::cout << "create vertex id index " << result << "\n";
    // } else {
    //     std::cerr << "failed create id index " << result << "\n";
    // }

}

void insert_edges(std::vector<std::string>& config, int therad_num) {
    RpcClient client(config[0], config[1], config[2]);
    std::mt19937 g(0);
    std::shuffle(edges.begin(), edges.end(), g);
    // ================== 并行批量导入边 ==================
    std::cout << "=== Starting to import edges in parallel ===" << "\n";
    auto start_time = std::chrono::high_resolution_clock::now();
    const size_t edge_batch_size = 10000;
    const size_t edge_report_interval = 100000;
    std::atomic<size_t> total_edges(0);
    // std::atomic<size_t> test_cnt(0);

    #pragma omp parallel num_threads(therad_num)
    {
        RpcClient thread_client(config[0], config[1], config[2]);
        // 使用静态调度，每个线程处理固定范围的边
        #pragma omp for schedule(static, edge_batch_size)
        for (size_t i = 0; i < edges.size(); i++) {
            size_t batch_start = (i / edge_batch_size) * edge_batch_size;
            size_t batch_end = std::min(batch_start + edge_batch_size, edges.size());
            // 确保每个线程只处理自己的批次
            if (i != batch_start) continue;
            // if (i != batch_start) {
            //     test_cnt.fetch_add(1);
            //     continue;
            // }
            std::string edges_json = "[";
            for (size_t j = batch_start; j < batch_end; j++) {
                if (j > batch_start) edges_json += ",";
                const auto& [u, v] = edges[j];
                std::string property = gen_len_x_p(u, v);
                std::string property5;
                for (int i = 0; i < 5; i ++) {
                    property5 += property;
                }
                edges_json += "{start_id:" + std::to_string(u) + 
                            ",end_id:" + std::to_string(v) +
                            ",f1:'" + property + "'" +
                            ",f2:'" + property5 + "'" +
                            ",f3:'" + property5 + "'" +
                            ",f4:'" + property5 + "'" +
                            ",f5:'" + property5 + "'" +
                            ",f6:'" + property5 + "'" +
                            ",f7:'" + property5 + "'" +
                            ",f8:'" + property5 + "'}";
            }
            edges_json += "]";
            
            std::string cypher = "CALL db.upsertEdge('Edge', "
                            "{type:'Vertex',key:'start_id'}, "
                            "{type:'Vertex',key:'end_id'}, " + 
                            edges_json + ")";
            
            std::string result;
            thread_client.CallCypher(result, cypher);
            // bool success = thread_client.CallCypher(result, cypher);
            // std::cout<<result<<"\n";
            
            // #pragma omp critical
            // {
            //     if (!success) {
            //         std::cerr << "Failed to import edges batch [" << batch_start << "-" << batch_end << "]: " << result << "\n";
            //     } else {
            //         size_t batch_count = batch_end - batch_start;
            //         total_edges += batch_count;
            //         if (batch_start % edge_report_interval == 0) {
            //             std::cout << "Imported " << batch_start << "/" << edges.size() << " edges" << "\n";
            //         }
            //     }
            // }
            // edges_json.clear();
            // edges_json.shrink_to_fit();
        }
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - start_time).count();

    std::string count_result;
    bool success = client.CallCypher(count_result, "MATCH ()-[e:Edge]->() RETURN count(e)");
    if (success) {
        auto edge_count = json::parse(count_result)[0]["count(e)"].get<size_t>();
        std::cout << "实际数据库中的边数量: " << edge_count << "\n";
    } else {
        std::cerr << "查询边数量失败: " << count_result << "\n";
    }

    std::cout << "\n=== Import Summary ===" << "\n";
    // std::cout << "test_cnt" << test_cnt << "\n";
    std::cout << "Total edges inserted: " << total_edges << "\n";
    std::cout << "insert Total time: " << duration << " ms" << "\n";
    std::cout << "insert QPS: " << static_cast<double>(total_edges) * 1000 / duration << " q/s" << "\n";
}

void update(std::vector<std::string>& config, int therad_num) {
    std::cout<<"begin to update!\n";
    // const size_t edge_batch_size = 1000;
    std::vector<int64_t> node_list(nodes.begin(), nodes.end());
    auto t1 = std::chrono::steady_clock::now();
    #pragma omp parallel num_threads(therad_num)
    {
        RpcClient thread_client(config[0], config[1], config[2]);
        // #pragma omp for schedule(static, edge_batch_size)
        #pragma omp for
        for(size_t i = 0; i < edges.size() / 1000; i++){
            int edge_id = random_uniform_int(0, edges.size() - 1);
            auto e = edges[edge_id];
            int64_t src = e.first;
            int64_t dst = e.second;
            std::string result;
            std::string cypher = gen_cypher_update_edge(src, dst);
            bool success = thread_client.CallCypher(result, cypher);
        }
    }
    auto t2 = std::chrono::steady_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);         
    std::cout<<"@update cost time:"<<time_span.count()<<"s\n";
    std::cout<<"@update qps:"<<edges.size() / 1000.0 / time_span.count()<<"\n";
}

void read(std::vector<std::string>& config, int therad_num) {
    std::cout<<"begin to read!\n";
    auto t1 = std::chrono::steady_clock::now();
    #pragma omp parallel num_threads(therad_num)
    {
        RpcClient thread_client(config[0], config[1], config[2]);
        #pragma omp for
        for(size_t i = 0; i < edges.size() / 1000; i++){
            int edge_id = random_uniform_int(0, edges.size() - 1);
            int sub_property_id = random_uniform_int(0, 7);
            auto e = edges[edge_id];
            int64_t src = e.first;
            int64_t dst = e.second;
            std::string result;
            std::string cypher = gen_cypher_get_edge_property(src, dst, sub_property_id);
            bool success = thread_client.CallCypher(result, cypher);
        }
    }
    auto t2 = std::chrono::steady_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);         
    std::cout<<"@read cost time:"<<time_span.count()<<"s\n";
    std::cout<<"@read qps:"<< edges.size() / 1000.0 / time_span.count()<<"\n";
}

std::vector<std::vector<uint64_t>> p99_store;
void deal_p99(){
  int tread_num = 16;
  std::vector<uint64_t>total_store;
  for(int i = 0; i < tread_num; i++){
    total_store.insert(total_store.end(), p99_store[i].begin(), p99_store[i].end());
  }
  std::sort(total_store.begin(), total_store.end());
  std::cout<<"p9  :"<<total_store[total_store.size() * 0.9]<<"\n";
  std::cout<<"p99 :"<<total_store[total_store.size() * 0.99]<<"\n";
  std::cout<<"p999:"<<total_store[total_store.size() * 0.999]<<"\n";
}

uint64_t getTimePoint() {
    auto now = std::chrono::system_clock::now();
    auto now_micros = std::chrono::time_point_cast<std::chrono::microseconds>(now);
    auto value = now_micros.time_since_epoch();
    return value.count();
}

void p99(std::vector<std::string> &config, int therad_num) {
    std::cout<<"begin to test p99!\n"; //这段单独进行测试
    p99_store.resize(16);
    int gap = 10000;
    const size_t edge_batch_size = 10000;
    const size_t edge_report_interval = 100000;
    #pragma omp parallel num_threads(therad_num)
    {
        RpcClient thread_client(config[0], config[1], config[2]);
        // #pragma omp for
        #pragma omp for schedule(static, edge_batch_size)
        for (size_t i = 0; i < edges.size(); i++) {
            size_t batch_start = (i / edge_batch_size) * edge_batch_size;
            size_t batch_end = std::min(batch_start + edge_batch_size, edges.size());
            // 确保每个线程只处理自己的批次
            if (i != batch_start) continue;
            auto e = edges[i];
            int64_t src = e.first;
            int64_t dst = e.second;
            std::string property = gen_len_x_p(src, dst);
            std::string property5;
            for (int i = 0; i < 5; i ++) {
                property5 += property;
            }
            std::string result;
            std::string cypher = 
                "MATCH (a:Vertex {id: " + std::to_string(src) + "}),"
                "(b:Vertex {id: " + std::to_string(dst) + "})"
                "CREATE (a)-[r:Edge {"
                "f1: '"+ property +"',"
                "f2: '"+ property5 +"',"
                "f3: '"+ property5 +"',"
                "f4: '"+ property5 +"',"
                "f5: '"+ property5 +"',"
                "f6: '"+ property5 +"',"
                "f7: '"+ property5 +"',"
                "f8: '"+ property5 +"'"
                "}]->(b);"
            ;
            auto t1 = getTimePoint();
            bool success = thread_client.CallCypher(result, cypher);
            if (!success) {
                std::cerr<< cypher <<"\n";
                std::cerr<< result <<"\n";
                assert(success);
            }
            auto t2 = getTimePoint();
            p99_store[omp_get_thread_num()].push_back(t2 - t1);
            std::string edges_json = "[";
            for (size_t j = batch_start+1; j < batch_end; j++) {
                if (j > batch_start+1) edges_json += ",";
                const auto& [u, v] = edges[j];
                std::string property = gen_len_x_p(u, v);
                std::string property5;
                for (int i = 0; i < 5; i ++) {
                    property5 += property;
                }
                edges_json += "{start_id:" + std::to_string(u) + 
                            ",end_id:" + std::to_string(v) +
                            ",f1:'" + property + "'" +
                            ",f2:'" + property5 + "'" +
                            ",f3:'" + property5 + "'" +
                            ",f4:'" + property5 + "'" +
                            ",f5:'" + property5 + "'" +
                            ",f6:'" + property5 + "'" +
                            ",f7:'" + property5 + "'" +
                            ",f8:'" + property5 + "'}";
            }
            edges_json += "]";
            
            cypher = "CALL db.upsertEdge('Edge', "
                            "{type:'Vertex',key:'start_id'}, "
                            "{type:'Vertex',key:'end_id'}, " + 
                            edges_json + ")";
            
            success = thread_client.CallCypher(result, cypher);
            if (!success) {
                std::cerr << "Failed to import edges batch [" << batch_start << "-" << batch_end << "]: " << result << "\n";
            }
        }
    }
    deal_p99();
}


void insert_read_alg(std::vector<std::string> &config, std::string data_path, int therad_num) {
    insert_nodes(config, data_path, therad_num);
    insert_edges(config, therad_num);
    read(config, therad_num);
    // run_app(config);
}

void insert_update_read_alg(std::vector<std::string> &config, std::string data_path, int therad_num) {
    insert_nodes(config, data_path, therad_num);
    insert_edges(config, therad_num);
    update(config, therad_num);
    read(config, therad_num);
    run_app(config);
}

void insert_p99(std::vector<std::string> &config, std::string data_path, int therad_num) {
    insert_nodes(config, data_path, therad_num);
    p99(config, therad_num);
}

int main() {
    try {
        // 从文件读取JSON
        std::ifstream config_file("../config.json");
        json config = json::parse(config_file);
        
        // 访问配置参数
        std::string data_path = config["path"];
        int test_case = config["test_case"];
        int thread_num = config["thread_num"];

        // 重定向标准输出到文件output.log
        freopen("output.log", "w", stdout); 

        const std::string url = "127.0.0.1:9090";
        const std::string user = "admin";
        const std::string password = "@73@TuGraph@";
        std::vector<std::string> client_config;
        client_config.push_back(url);
        client_config.push_back(user);
        client_config.push_back(password);

        switch (test_case) {
            case 1:
                insert_read_alg(client_config, data_path, thread_num);
            break;
            case 2:
                insert_update_read_alg(client_config, data_path, thread_num);
            break;
            case 3:
                insert_p99(client_config, data_path, thread_num);
            break;
        }
        
        // 恢复标准输出到控制台（可选，若后续还需要在控制台输出信息）
        fclose(stdout);
        freopen("CON", "w", stdout); 

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}