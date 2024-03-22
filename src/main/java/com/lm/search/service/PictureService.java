package com.lm.search.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.lm.search.model.entity.Picture;

/**
 * 图片服务
 *
 * @author lm
 * @from lm
 */
public interface PictureService {

    Page<Picture> searchPicture(String searchText, long pageNum, long pageSize);
}
