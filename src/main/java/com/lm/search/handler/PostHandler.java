package com.lm.search.handler;

import com.lm.search.model.dto.post.PostEsDTO;
import com.lm.search.model.entity.Post;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.stereotype.Component;
import top.javatool.canal.client.annotation.CanalTable;
import top.javatool.canal.client.handler.EntryHandler;

import javax.annotation.Resource;

/**
 * MySQL同步更新到ES
 * @Author lm
 * @Date 2024-03-02 18:03
 */
@CanalTable("post")
@Component
@AllArgsConstructor
@Slf4j
public class PostHandler implements EntryHandler<Post> {
    @Resource
    private final ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Override
    public void insert(Post post) {
        PostEsDTO postEsDTO = new PostEsDTO();
        BeanUtils.copyProperties(post,postEsDTO);
        elasticsearchRestTemplate.save(postEsDTO);
    }

    @Override
    public void update(Post before, Post after) {
        PostEsDTO postEsDTO = new PostEsDTO();
        BeanUtils.copyProperties(after,postEsDTO);
        elasticsearchRestTemplate.save(postEsDTO);
    }

    @Override
    public void delete(Post post) {
        elasticsearchRestTemplate.delete(post.getId().toString(),PostEsDTO.class);
    }
}
