package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result typeList() {
        //1.从redis缓存中得到店铺类型
        String key = "cache:shopType:";
        List<String> list = stringRedisTemplate.opsForList().range(key, 0, -1);
        //2.有就返回
        if (CollectionUtil.isNotEmpty(list)) {
            //redis查出来是string类型，转换成shopType
            List<ShopType> shopTypes = JSONUtil.toList(list.toString(), ShopType.class);
            Collections.sort(shopTypes,((o1, o2) -> o1.getSort() - o2.getSort()));
            return Result.ok(shopTypes);
        }
        //3.没有从数据库中找
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        //4.存到redis
        List<String> shopTypesJson = shopTypes.stream()
                .map(shopType -> JSONUtil.toJsonStr(shopType))
                .collect(Collectors.toList());
        stringRedisTemplate.opsForList().rightPushAll(key,shopTypesJson);
        //5.返回
        return Result.ok(shopTypes);
    }
}
