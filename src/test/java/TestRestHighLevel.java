import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class TestRestHighLevel {

    private RestHighLevelClient client;

    private final static String INDEX = "haoke";
    private final static String TYPE = "house";


    @Before
    public void init() {
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("vagrant", 9200, "http"),
                new HttpHost("vagrant", 9201, "http"),
                new HttpHost("vagrant", 9202, "http")
        );

        this.client = new RestHighLevelClient(restClientBuilder);
    }

    @After
    public void after() throws IOException {
        this.client.close();
    }

    @Test
    public void testCreate() throws IOException {
        Map<String, Object> data = new HashMap<>();
        data.put("id", "2002");
        data.put("title", "南京西路 零包入住 一室一厅");
        data.put("price", 4500);

        IndexRequest indexRequest = new IndexRequest("haoke", "house").source(data);

        IndexResponse indexResponse = this.client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("id->" + indexResponse.getId());
        System.out.println("index->" + indexResponse.getIndex());
        System.out.println("type->" + indexResponse.getType());
        System.out.println("version->" + indexResponse.getVersion());
        System.out.println("result->" + indexResponse.getResult());
        System.out.println("shardInfo->" + indexResponse.getShardInfo());
    }

    @Test
    public void testCreateAsync() throws IOException, InterruptedException {
        Map<String, Object> data = new HashMap<>();
        data.put("id", "2003");
        data.put("title", "南京北路 零包入住 一室一厅");
        data.put("price", 4600);

        IndexRequest indexRequest = new IndexRequest("haoke", "house").source(data);

        this.client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println("id->" + indexResponse.getId());
                System.out.println("index->" + indexResponse.getIndex());
                System.out.println("type->" + indexResponse.getType());
                System.out.println("version->" + indexResponse.getVersion());
                System.out.println("result->" + indexResponse.getResult());
                System.out.println("shardInfo->" + indexResponse.getShardInfo());
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("遇到异常");
            }
        });

        Thread.sleep(999999L);
    }

    @Test
    public void testQuery() throws IOException {
        GetRequest getRequest = new GetRequest("haoke", "house", "LxZPynsBp7XT5sc7qE_E");

        // 指定返回固定字段
        String[] includes = new String[]{"title", "id"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);

        // 查询
        GetResponse getResponse = this.client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println("数据 -> " + getResponse.getSource());
    }

    @Test
    public void testExists() throws IOException {
        GetRequest getRequest = new GetRequest("haoke", "house", "LxZPynsBp7XT5sc7qE_E1");

        // 不返回字段
        getRequest.fetchSourceContext(new FetchSourceContext(false));

        boolean exists = this.client.exists(getRequest, RequestOptions.DEFAULT);

        System.out.println("exists:" + exists);
    }

    @Test
    public void testDelete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("haoke", "house","LRZMynsBp7XT5sc7L092");
        DeleteResponse deleteResponse = this.client.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(deleteResponse.status());
    }

    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, "LhZPynsBp7XT5sc7H0-B");
        Map<String, Object> data = new HashMap<>();
//        data.put("title", "北京昌平");
        data.put("price", 5100);

        updateRequest.doc(data);

        UpdateResponse updateResponse = this.client.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println("result:" + updateResponse.getResult());
        System.out.println("version:" + updateResponse.getVersion());
    }

    @Test
    public void testSearch() throws IOException {
        SearchRequest request = new SearchRequest(INDEX);
        request.types(TYPE);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title", "拎包入住"));
        searchSourceBuilder.from(1);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));


        request.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

        System.out.println("查询到:" + searchResponse.getHits().getTotalHits());
        System.out.println("result:" + searchResponse.getProfileResults());

        SearchHits searchHits = searchResponse.getHits();
        searchHits.forEach((item)->{
            System.out.println(item.getSourceAsString());

        });

    }
}
