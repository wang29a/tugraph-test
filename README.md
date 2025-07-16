

运行docker

```sh
docker run -d --rm \
  -p 7071:7070 \
  -p 7688:7687 \
  -p 9091:9090 \
  -v \tugraph\data:/var/lib/lgraph/data \
  -v \tugraph\log:/var/log/lgraph_log \
  --name tugraph_dev `
  tugraph/tugraph-runtime-ubuntu18.04
```

```sh
lgraph_server -c /usr/local/etc/lgraph.json -d start 
```

导出依赖文件

```sh
mkdir -p ./lib
docker cp tugraph-dev:/usr/local/include/lgraph ./
docker cp tugraph-dev:/usr/local/include/tools  ./
docker cp tugraph-dev:/usr/local/lib64/liblgraph_client_cpp_rpc.so ./lib/
```


修改 config.json中的data path、测试模式

测试模式 test_case
1 插入->读取->图算法
2 插入->更新->读取->图算法
3 插入(测试test_p99)


使用的cypher
```cypher

查询边

更新边

查询顶点所有出边
MATCH (n)-[r]->(m)
WHERE n.id = 123
RETURN r, m.id as target_id

MATCH (n:Vertex{id:0})-[r]->(m)
RETURN r.f1 as rf

k-hop
MATCH (n:Vertex{id:0})-[r:Edge*2]->(m)
RETURN m.id as dst


```