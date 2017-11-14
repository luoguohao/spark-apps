package com.luogh.spark.util;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * @author luogh
 */
public class JavaUtil {

  public static List<String> readLines(String fileName) throws IOException {
    List<String> lines = Lists.newArrayList();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName),
        Charsets.UTF_8.name()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    return lines;
  }

}
