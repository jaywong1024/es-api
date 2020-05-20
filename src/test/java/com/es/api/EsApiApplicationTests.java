package com.es.api;

import com.alibaba.fastjson.JSON;
import com.es.api.pojo.Phone;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Optional;

@SpringBootTest
class EsApiApplicationTests {

    /**
     * 在 ElasticsearchRestClientConfigurations.RestHighLevelClientConfiguration 中
     * 也配置了 RestHighLevelClient 类型的 Bean，使用 @Qualifier 注解进行区分
     */
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 1. 创建索引
     */
    @Test
    void testCreateIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("phone");
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(response));
    }

    /**
     * 2. 索引是否存在
     */
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("phone");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 3. 删除索引
     */
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("phone");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    /**
     * 4. 添加文档
     */
    @Test
    void testAddDocument() throws IOException {
        Phone iPhoneXR = new Phone("Apple iPhone XR", "A12", "3G", "64G");
        IndexRequest request = new IndexRequest("phone");
        request.id("iPhoneXR");
        request.timeout(TimeValue.timeValueSeconds(1));
//        添加 JSON 对象
        request.source(JSON.toJSONString(iPhoneXR), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.status()); // CREATED
    }

    /**
     * 5. 判断文档是否存在
     */
    @Test
    void testExistDocument() throws IOException {
        GetRequest request = new GetRequest("phone", "iPhoneXR");
//        不获取文档上下文
        request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        request.storedFields("_none_");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 6. 获取文档信息
     */
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("phone", "iPhoneXR");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    /**
     * 7. 更新文档信息
     */
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("phone", "iPhoneXR");
        request.doc(JSON.toJSONString(
                new Phone("萌萌哒的XR酱", "A12", "3G", "64G")
        ), XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.status()); // OK
    }

    /**
     * 8. 删除文档记录
     */
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("phone", "iPhoneXR");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status()); // OK
    }

    /**
     * 9. 批量添加文档
     */
    @Test
    void testBulkAddDocument() throws IOException {
        BulkRequest request = new BulkRequest("phone");
        request.timeout("10s");
        for (int i = 1; i <= 10; i++) {
            request.add(new IndexRequest().id("iPhone" + i).
                    source(JSON.toJSONString(
                            new Phone("Apple iPhone" + i, "A" + i, i + "G", i + "G")
                    ), XContentType.JSON)
            );
        }
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(responses.status()); // OK
    }

    /**
     * 10. 批量更新文档
     */
    @Test
    void testBulkUpdateDocument() throws IOException {
        BulkRequest request = new BulkRequest("phone");
        request.timeout("10s");
        for (int i = 1; i <= 10; i++) {
            request.add(new UpdateRequest().id("iPhone" + i).
                    doc(JSON.toJSONString(
                            new Phone("Apple iPhone" + i + "_update", "A" + i, i + "G", i + "G")
                    ), XContentType.JSON)
            );
        }
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(responses.status()); // OK
    }

    /**
     * 11. 批量删除文档
     */
    @Test
    void testBulkDeleteDocument() throws IOException {
        BulkRequest request = new BulkRequest("phone");
        request.timeout("10s");
        for (int i = 1; i <= 10; i++) {
            request.add(new DeleteRequest().id("iPhone" + i));
        }
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(responses.status()); // OK
    }

    /**
     * 12. 查询
     * - SearchRequest          查询请求
     * - SearchSourceBuilder    查询条件构建器
     *      - query             添加查询条件
     *      - highlighter       高亮
     *      - from, size        分页
     */
    @Test
    void testBulkSearch() throws IOException {
        SearchRequest request = new SearchRequest("phone");
//        查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.timeout(TimeValue.timeValueSeconds(60));
//        匹配
        builder.query(new BoolQueryBuilder().must(new MatchQueryBuilder("cpu", "A12")))
//        高亮
        .highlighter(new HighlightBuilder().field("cpu")
                .preTags("<span style='color: red;'>").postTags("</span>"))
//        分页
        .from(0).size(10);

//        将查询条件写入请求
        SearchResponse response = client.search(request.source(builder), RequestOptions.DEFAULT);

//        解析高亮
        for (SearchHit hit : response.getHits().getHits()) {
//            将高亮字段设置回结果中
            hit.getSourceAsMap().put("cpu",
                    Optional.ofNullable(
                            hit.getHighlightFields().get("cpu").getFragments()[0].toString()
                    ).orElse(
                            hit.getSourceAsMap().get("cpu").toString()
                    )
            );
        }

        System.out.println(JSON.toJSONString(response.getHits()));

    }

}
