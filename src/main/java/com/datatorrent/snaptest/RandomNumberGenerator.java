/**
 * Put your copyright and license info here.
 */
package com.datatorrent.snaptest;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<List<Map<String, Object>>> out = new DefaultOutputPort<>();

  public static final String A = "a";
  public static final String B = "b";
  public static final String C = "c";
  public static final String D = "d";

  public static final String A_NAME = "a name";
  public static final String B_NAME = "b name";
  public static final String C_NAME = "c name";
  public static final String D_NAME = "d name";

  public static final List<String> NAMES_A = Lists.newArrayList("bill", "bob", "tom", "todd", "willy", "nilly");
  public static final List<String> NAMES_B = Lists.newArrayList("chocolate", "banana", "coconut", "apple", "pie", "coffee");
  public static final List<String> NAMES_C = Lists.newArrayList("panda", "penguin", "platapus", "dodo", "octopus", "salamander");
  public static final List<String> NAMES_D = Lists.newArrayList("Google", "Data Torrent", "Drop Box", "Face Book", "Cisco", "Oracle");


  private transient Random rand = new Random();

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void emitTuples()
  {
  }

  @Override
  public void endWindow()
  {
    Long a = 0L;
    Long b = 0L;
    Long c = 0L;
    Double d = 0.0;

    List<Map<String, Object>> results = Lists.newArrayList();

    for (int counter = 0; counter < 6; counter++) {
      Map<String, Object> resultMap = Maps.newHashMap();

      a += rand.nextInt(5);
      b += rand.nextInt(5);
      c += rand.nextInt(5);
      d += rand.nextDouble() * 5;

      resultMap.put(A, a);
      resultMap.put(B, b);
      resultMap.put(C, c);
      resultMap.put(D, d);

      resultMap.put(A_NAME, NAMES_A.get(counter));
      resultMap.put(B_NAME, NAMES_B.get(counter));
      resultMap.put(C_NAME, NAMES_C.get(counter));
      resultMap.put(D_NAME, NAMES_D.get(counter));
    }

    out.emit(results);
  }
}
