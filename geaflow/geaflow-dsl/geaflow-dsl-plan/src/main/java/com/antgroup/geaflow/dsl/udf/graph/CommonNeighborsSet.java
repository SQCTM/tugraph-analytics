/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

@Description(name = "common_neighbors_for_vertex_set", description = "built-in udga for CommonNeighborsForVertexSet")
public class CommonNeighborsSet implements AlgorithmUserFunction<Object, Tuple<Boolean, Boolean>> {

    private AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context;
    private HashSet<Object> A = new HashSet<>(); // A集合
    private HashSet<Object> B = new HashSet<>(); // B集合

    @Override
    public void init(AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context, Object[] params) {
        this.context = context;

        // 参数格式：params[0] = "1,2,3", params[1] = "4,5,6"
        if (params.length < 2) {
            throw new IllegalArgumentException("Need two vertex sets as parameters");
        }

        // 解析两个集合的参数
        String[] aIds = params[0].toString().split(",");
        String[] bIds = params[1].toString().split(",");

        for (String id : aIds) {
            A.add(id.trim());
        }
        for (String id : bIds) {
            B.add(id.trim());
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Boolean, Boolean>> messages) {

        Object vertexId = vertex.getId();

        if (context.getCurrentIterationId() == 1L) {
            // 第1轮：A/B集合向邻居发送标识消息
            Tuple<Boolean, Boolean> flag = new Tuple<>(false, false);
            if (A.contains(vertexId)) {
                flag.setF0(true); // 来自A集合
                sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH), flag);
            } else if (B.contains(vertexId)) {
                flag.setF1(true); // 来自B集合
                sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH), flag);
            }

        } else if (context.getCurrentIterationId() == 2L) {
            // 第2轮：统计收到的消息，判断是否是共同邻居
            boolean fromA = false;
            boolean fromB = false;

            while (messages.hasNext()) {
                Tuple<Boolean, Boolean> m = messages.next();
                if (m.getF0()) fromA = true;
                if (m.getF1()) fromB = true;
            }

            if (fromA && fromB) {
                // 是A和B的共同邻居，输出
                context.take(ObjectRow.create(vertex.getId()));
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        // 每个顶点最后一轮逻辑，不需要处理时可以留空
    }

    @Override
    public void finish() {
        AlgorithmUserFunction.super.finish();
    }

    @Override
    public void finishIteration(long iterationId) {
        AlgorithmUserFunction.super.finishIteration(iterationId);
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Tuple<Boolean, Boolean> message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
                new TableField("id", graphSchema.getIdType(), false)
        );
    }
}
