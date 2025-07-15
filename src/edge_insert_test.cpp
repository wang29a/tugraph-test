// #include "bitmap.hpp"
// #include "atomic.hpp"
#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include <string>
#include <lgraph/lgraph_rpc_client.h>
#include "tools/json.hpp"
#include <omp.h>
#include <random>
#include <unordered_set>
#include <sstream>
#include <atomic>

using namespace std;
using namespace lgraph;
using json = nlohmann::json;
using value_t = int64_t;

thread_local mt19937 gen(random_device{}());
string generate_random_five_digit() {
    uniform_int_distribution<> dist(10000, 99999);
    return to_string(dist(gen));
}

vector<pair<int64_t, int64_t>> edges;
unordered_set<int64_t> nodes;

// 辅助函数：拼接向量元素为字符串（用分隔符连接）
template <typename InputIt>
string join(InputIt first, InputIt last, const string& delimiter) {
    if (first == last) return "";
    ostringstream ss;
    copy(first, last, ostream_iterator<string>(ss, delimiter.c_str()));
    string result = ss.str();
    return result.substr(0, result.size() - delimiter.size()); // 移除最后一个分隔符
}

void ReadEdgesWithNodes(const string& file_path) {
    ifstream file(file_path, ios::binary);
    if (!file.is_open()) {
        throw runtime_error("Failed to open edge file: " + file_path);
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
    
    // cout<<"最大点id："<<mx<<endl;
    
    file.close();
    cout << "Read " << edges.size() << " edges and " << nodes.size() << " unique nodes." << endl;
}

void ReadEdgesWithNodesTest(const string& file_path) {
    ifstream file(file_path);
    if (!file.is_open()) {
        throw runtime_error("Failed to open edge file: " + file_path);
    }
    
    string line;
    while (getline(file, line)) {
        istringstream iss(line);
        int src, dst;
        if (iss >> src>>dst) {
            edges.emplace_back(src, dst);
            nodes.insert(src);
            nodes.insert(dst);
        } else {
            cerr<< "Warning: Invalid line format: " << line <<"\n";
        }
    }
    file.close();
    cout << "Read " << edges.size() << " edges and " << nodes.size() << " unique nodes." << endl;
}

std::string gen_len_x_p(int64_t src, int64_t dst){ //长度为1 * 5 + 7 * 20 ，用|分隔 
    int dig = (src + dst) % 100000;
    std::string ans = std::to_string(dig);
    while(ans.size() < 5){
        ans = "0" + ans;
    }
    return ans;
}

void run_app(){
      /******************************** Test APP ********************************/
    std::string alg_app;
    int repeat_time = 4;
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
        std::cout << "  alg_app: " << alg_app << std::endl;
        std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
        int64_t vertex_num = nodes.size();
        // double before = utils::get_memory_usage();
        if (alg_app == "sssp") {
          // for(int j = 0; j < graphdata.node_num; j += graphdata.node_num / 100){
          //   run_sssp(txn, seq, 0, j);
          // }
        // TODO  run_sssp(txn, seq, 0, FLAGS_source);
        } else if (alg_app == "scan") {
        // TODO  run_scan(txn, seq);
        } else if (alg_app == "khop") {
        // TODO  run_khop(txn, FLAGS_khop, seq);
        } else if (alg_app == "khop_plus") {
        // TODO  run_khop_plus(txn, FLAGS_khop, seq, 0, 50000);
        } else {
          std::cout << "No this type, alg_app_type=" << alg_app << std::endl;
        }
        std::chrono::steady_clock::time_point t2 
            = std::chrono::steady_clock::now();
        std::chrono::duration<double> time_span 
            = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        all_run_time += time_span.count();
        // double over = utils::get_memory_usage();
        std::cout << " run_time=" << time_span.count() 
                //   << " run_mem=" << (over - before)
                  << std::endl;
        // utils::get_io_info(alg_app + "_" + std::to_string(i) + "_");
      }

      std::cout << "Run time(sec): " << std::endl;
      std::cout << "  @" << alg_app << ": " 
                << all_run_time / repeat_time
                << std::endl;
    //   double over = utils::get_memory_usage();
    //   printf("  %s Memory usage(GB): %.3f\n", alg_app.c_str(), over - before);
    }
}

int main() {
    try {
        // 重定向标准输出到文件output.log
        freopen("output.log", "w", stdout); 

        const string url = "127.0.0.1:9090";
        const string user = "admin";
        const string password = "@73@TuGraph@";
        RpcClient client(url, user, password);

        // 清除原有数据库
        string str;
        bool res = client.CallCypher(str, "CALL db.dropDB()");
        cout << "Drop database result: " << (res ? "success" : "failed") << endl;

        // 创建顶点标签（添加RETURN语句以减少输出）
        res = client.CallCypher(str, 
            "CALL db.createVertexLabel('Vertex','id','id', int64, false) RETURN 'Label created'");
        cout << "Vertex label creation result: " << (res ? "success" : "failed") << endl;

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
        cout << "Edge label creation result: " << (res ? "success" : "failed") << endl;

        // ReadEdgesWithNodes("/mnt/tugraph/twitter-2010.txt.b");
        ReadEdgesWithNodesTest("../web-Google.txt/web-Google.txt");

        
        // int mx=0;
        // for(auto x:nodes)mx=max(mx,x);
        // cout<<"最大的点编号："<<mx<<endl;
        
        // ================== 并行批量导入顶点 ==================
        cout << "=== Starting to import nodes in parallel ===" << endl;
        vector<int64_t> node_list(nodes.begin(), nodes.end());
        atomic<size_t> total_nodes(0);
        const size_t node_batch_size = 100000;
        const size_t node_report_interval = 100000;
        auto start_time = chrono::high_resolution_clock::now();
        
        #pragma omp parallel num_threads(8)
        {
            RpcClient thread_client(url, user, password);
            
            // 使用静态调度，每个线程处理固定范围的顶点
            #pragma omp for schedule(static, node_batch_size)
            for (size_t i = 0; i < node_list.size(); i++) {
                size_t batch_start = (i / node_batch_size) * node_batch_size;
                size_t batch_end = min(batch_start + node_batch_size, node_list.size());
                
                // 确保每个线程只处理自己的批次
                if (i != batch_start) continue;
                
                string nodes_json = "[";
                for (size_t j = batch_start; j < batch_end; j++) {
                    if (j > batch_start) nodes_json += ",";
                    nodes_json += "{id:" + to_string(node_list[j]) + "}";
                }
                nodes_json += "]";
                
                string cypher = "CALL db.upsertVertex('Vertex', " + nodes_json + ")";
                string result;
                bool success = thread_client.CallCypher(result, cypher);
                
                #pragma omp critical
                {
                    if (!success) {
                        cerr << "Failed to import nodes batch [" << batch_start << "-" << batch_end << "]: " << result << endl;
                    } else {
                        size_t batch_count = batch_end - batch_start;
                        total_nodes += batch_count;
                        if (batch_start % node_report_interval == 0) {
                            cout << "Imported " << batch_start << "/" << node_list.size() << " nodes" << endl;
                        }
                    }
                }
            }
        }
        cout << "Successfully imported " << total_nodes << " nodes in parallel." << endl;
        nodes.clear();

        // ================== 并行批量导入边 ==================
        cout << "=== Starting to import edges in parallel ===" << endl;
        const size_t edge_batch_size = 10000;
        const size_t edge_report_interval = 100000;
        atomic<size_t> total_edges(0);

        #pragma omp parallel num_threads(8)
        {
            RpcClient thread_client(url, user, password);
            
            // 使用静态调度，每个线程处理固定范围的边
            #pragma omp for schedule(static, edge_batch_size)
            for (size_t i = 0; i < edges.size(); i++) {
                size_t batch_start = (i / edge_batch_size) * edge_batch_size;
                size_t batch_end = min(batch_start + edge_batch_size, edges.size());
                
                // 确保每个线程只处理自己的批次
                if (i != batch_start) continue;
                
                string edges_json = "[";
                for (size_t j = batch_start; j < batch_end; j++) {
                    if (j > batch_start) edges_json += ",";
                    const auto& [u, v] = edges[j];
                    string property = gen_len_x_p(u, v);
                    string property5;
                    for (int i = 0; i < 5; i ++) {
                        property5 += property;
                    }
                    edges_json += "{start_id:" + to_string(u) + 
                                ",end_id:" + to_string(v) +
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
                
                string cypher = "CALL db.upsertEdge('Edge', "
                              "{type:'Vertex',key:'start_id'}, "
                              "{type:'Vertex',key:'end_id'}, " + 
                              edges_json + ")";
                
                string result;
                bool success = thread_client.CallCypher(result, cypher);
                cout<<result<<"\n";
                
                #pragma omp critical
                {
                    if (!success) {
                        cerr << "Failed to import edges batch [" << batch_start << "-" << batch_end << "]: " << result << endl;
                    } else {
                        size_t batch_count = batch_end - batch_start;
                        total_edges += batch_count;
                        if (batch_start % edge_report_interval == 0) {
                            cout << "Imported " << batch_start << "/" << edges.size() << " edges" << endl;
                        }
                    }
                }
                // edges_json.clear();
                // edges_json.shrink_to_fit();
            }
        }
        string count_result;
        bool success = client.CallCypher(count_result, "MATCH (n:Vertex) RETURN count(n)");
        if (success) {
            auto node_count = json::parse(count_result)[0]["count(n)"].get<size_t>();
            cout << "实际数据库中的顶点数量: " << node_count << endl;
        } else {
            cerr << "查询顶点数量失败: " << count_result << endl;
        }

        success = client.CallCypher(count_result, "MATCH ()-[e:Edge]->() RETURN count(e)");
        if (success) {
            auto edge_count = json::parse(count_result)[0]["count(e)"].get<size_t>();
            cout << "实际数据库中的边数量: " << edge_count << endl;
        } else {
            cerr << "查询边数量失败: " << count_result << endl;
        }

        auto duration = chrono::duration_cast<chrono::milliseconds>(
            chrono::high_resolution_clock::now() - start_time).count();
        
        cout << "\n=== Import Summary ===" << endl;
        cout << "Total edges inserted: " << total_edges << endl;
        cout << "Total time: " << duration << " ms" << endl;
        cout << "QPS: " << static_cast<double>(total_edges) * 1000 / duration << " q/s" << endl;
        
        // edges.clear();
        
        // if (1) {
        //     // int64_t source = edges[0].first; // 使用第一条边的起点作为源节点
        //     cout<<"开始求单源最短路"<<endl;
        //     auto start_time1 = chrono::high_resolution_clock::now();
        //     run_sssp(client, 1);
        //      auto duration1 = chrono::duration_cast<chrono::milliseconds>(
        //     chrono::high_resolution_clock::now() - start_time).count();
        //     cout << "\n=== SSSP Summary ===" << endl;
        //     cout << "Total time: " << duration1 << " ms" << endl;
        // }
        
        // 恢复标准输出到控制台（可选，若后续还需要在控制台输出信息）
        fclose(stdout);
        freopen("CON", "w", stdout); 

    } catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << endl;
        return 1;
    }
    return 0;
}