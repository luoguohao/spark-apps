package com.luogh.spark.util;

import com.luogh.spark.entity.Poi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by wyy on 2017/8/18.
 */
public class HotelTool {

  private static final Logger logger = Logger.getLogger(HotelTool.class);
  private static final String threebrand = "三星级宾馆";
  private static final String otherbrand = "经济型连锁酒店";
  private static final String five = "五星级宾馆";
  private static final String four = "四星级宾馆";
  private static final String three = "三星级宾馆";
  private static Map<String, List<String>> brandMap = new HashMap<>();

  static {
    try {
      List<String> threebrandList = JavaUtil.readLines("threebrand.txt");
      brandMap.put(threebrand, threebrandList);

      List<String> otherbrandList = JavaUtil.readLines("threebrand.txt");
      brandMap.put(otherbrand, otherbrandList);

      logger.info("三星级宾馆一共=" + threebrandList.size() + '个');
      logger.info("经济型连锁酒店一共=" + otherbrandList.size() + '个');

    } catch (IOException e) {
      logger.error("init error,msg=" + e.getMessage());
      e.printStackTrace();
    }
  }

  public static List<Poi> getHotel(List<Poi> poiList) {
    List<Poi> resultPoilist = new ArrayList<>();

    if (poiList == null || poiList.size() == 0) {
      return resultPoilist;
    }

    if (poiList.size() == 1) {
      resultPoilist.add(normalBrand(poiList.get(0)));
      return resultPoilist;
    }

    int fiveIndex = -1;
    int fourIndex = -1;
    int threeIndex = -1;

    for (int i = 0; i < poiList.size(); i++) {
      Poi poi = poiList.get(i);
      if (StringUtils.isNotEmpty(poi.getBrand()) && poi.getBrand().contains(five)) {
        fiveIndex = i;
      }

      if (StringUtils.isNotEmpty(poi.getBrand()) && poi.getBrand().contains(four)) {
        fourIndex = i;
      }

      if (StringUtils.isNotEmpty(poi.getBrand()) && poi.getBrand().contains(three)) {
        threeIndex = i;
      }
    }

    if (fiveIndex >= 0) {
      resultPoilist.add(normalBrand(poiList.get(fiveIndex)));
      return resultPoilist;
    }

    if (fourIndex >= 0) {
      resultPoilist.add(normalBrand(poiList.get(fourIndex)));
      return resultPoilist;
    }

    if (threeIndex >= 0) {
      resultPoilist.add(normalBrand(poiList.get(threeIndex)));
      return resultPoilist;
    }

    resultPoilist.add(normalBrand(poiList.get(0)));
    return resultPoilist;
  }


  /***
   * 标准化品牌
   * @param poi
   */
  public static Poi normalBrand(Poi poi) {

    for (String key : brandMap.keySet()) {
      if (poi.getType().contains(otherbrand) || poi.getType().contains(threebrand)) {
        List<String> brandList = brandMap.get(key);
        for (String brand : brandList) {
          if (StringUtils.isNotEmpty(brand)) {
            if (StringUtils.isNotEmpty(poi.getBrand()) && poi.getBrand().contains(brand)) {
              poi.setBrand(brand);
              poi.setType(key);
              return poi;
            }
          }
        }
      }
    }

    for (String key : brandMap.keySet()) {
      if (poi.getType().contains(key)) {
        poi.setBrand("");
      }
    }
    return poi;
  }
}
