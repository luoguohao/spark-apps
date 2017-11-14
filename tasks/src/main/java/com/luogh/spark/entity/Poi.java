package com.luogh.spark.entity;

/**
 * Created by wyy on 2017/8/1.
 */
public class Poi {

  private String poiid;
  private String lat;
  private String lng;
  private String name;
  private String brand;
  private String price;
  private String type;
  private String province;
  private String city;
  private String district;

  public Poi(String poiid, String lat, String lng, String name, String brand, String price,
      String type, String province, String city, String district) {
    this.poiid = poiid;
    this.lat = lat;
    this.lng = lng;
    this.name = name;
    this.brand = brand;
    this.price = price;
    this.type = type;
    this.province = province;
    this.city = city;
    this.district = district;
  }

  public String getPrice() {
    return price;
  }

  public void setPrice(String price) {
    this.price = price;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getProvince() {
    return province;
  }

  public void setProvince(String province) {
    this.province = province;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getDistrict() {
    return district;
  }

  public void setDistrict(String district) {
    this.district = district;
  }

  public String getPoiid() {
    return poiid;
  }

  public void setPoiid(String poiid) {
    this.poiid = poiid;
  }

  public String getLat() {
    return lat;
  }

  public void setLat(String lat) {
    this.lat = lat;
  }

  public String getLng() {
    return lng;
  }

  public void setLng(String lng) {
    this.lng = lng;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getBrand() {
    return brand;
  }

  public void setBrand(String brand) {
    this.brand = brand;
  }

}
