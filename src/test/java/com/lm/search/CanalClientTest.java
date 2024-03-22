package com.lm.search;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.lm.search.esdao.PostEsDao;
import com.lm.search.model.dto.post.PostEsDTO;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@SpringBootTest
public class CanalClientTest {
    @Resource
    private PostEsDao postEsDao;
    @Test
    void canalConnect() throws InterruptedException, InvalidProtocolBufferException, JsonProcessingException, ParseException {
        // 创建canal客户端，单链接模式
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                11111), "example", "", "");
        // 创建连接
        canalConnector.connect();
        while (true) {
            // 订阅数据库
            // canalConnector.subscribe("mall");

            // 获取数据
            Message message = canalConnector.get(100);

            // 获取Entry集合
            List<CanalEntry.Entry> entries = message.getEntries();

            // 判断集合是否为空,如果为空,则等待一会继续拉取数据
            if (entries.size() <= 0) {
//                System.out.println("当次抓取没有数据，休息一会。。。。。。");
                Thread.sleep(1000);
            } else {
                // 遍历entries，单条解析
                for (CanalEntry.Entry entry : entries) {

                    // 1.获取表名
                    String tableName = entry.getHeader().getTableName();

                    // 2.获取类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    // 3.获取序列化后的数据
                    ByteString storeValue = entry.getStoreValue();

                    // 4.判断当前entryType类型是否为ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {

                        // 5.反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        // 6.获取当前事件的操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        // 7.获取数据集
                        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

                        // 8.遍历rowDataList，并打印数据集
                        for (CanalEntry.RowData rowData : rowDataList) {

                            JSONObject beforeData = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                beforeData.put(column.getName(), column.getValue());
                            }

                            JSONObject afterData = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            PostEsDTO postEsDTO = new PostEsDTO();
                            for (CanalEntry.Column column : afterColumnsList) {
                                afterData.put(column.getName(), column.getValue());
                            }
                            if (tableName.equals("post")) {
                                for (CanalEntry.Column column : afterColumnsList) {
                                    if (column.getName().equals("id")) {
                                        postEsDTO.setId(Long.parseLong(column.getValue()));
                                    }
                                    if (column.getName().equals("title")) {
                                        postEsDTO.setTitle(column.getValue());
                                    }
                                    if (column.getName().equals("content")) {
                                        postEsDTO.setContent(column.getValue());
                                    }
                                    if (column.getName().equals("tags")) {
                                        ObjectMapper objectMapper = new ObjectMapper();
                                        List<String> list = objectMapper.readValue(column.getValue(), new TypeReference<List<String>>() {});
                                        postEsDTO.setTags(list);
                                        System.out.println("11111111111111111111111111111111111111111");
                                    }
                                    if (column.getName().equals("userId")) {
                                        postEsDTO.setUserId(Long.parseLong(column.getValue()));
                                    }
                                    if (column.getName().equals("isDelete")) {
                                        postEsDTO.setIsDelete(Integer.parseInt(column.getValue()));
                                    }
                                    if (column.getName().equals("createTime")) {
                                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                        Date date = simpleDateFormat.parse(column.getValue());
                                        postEsDTO.setCreateTime(date);
                                    }
                                    if (column.getName().equals("updateTime")) {
                                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                        Date date = simpleDateFormat.parse(column.getValue());
                                        postEsDTO.setUpdateTime(date);
                                    }
                                }
                                postEsDao.save(postEsDTO);
                                System.out.println(postEsDTO);
                                System.out.println("修改了post");
                            }

                            // 数据打印
                            System.out.println("Table:" + tableName +
                                    ",EventType:" + eventType +
                                    ",Before:" + beforeData +
                                    ",After:" + afterData);
                        }
                    }
                }
            }
        }
    }
}