-- 创建结果表
CREATE TABLE result_tb (
                           vid int
) WITH (
      type='file',
      geaflow.dsl.file.path='${target}'
      );
-- 使用modern图
USE GRAPH modern;
-- 调用算法并插入结果
INSERT INTO result_tb
    CALL common_neighbors_for_vertex_set('3', '2,5') YIELD (id)
RETURN cast (id as int);