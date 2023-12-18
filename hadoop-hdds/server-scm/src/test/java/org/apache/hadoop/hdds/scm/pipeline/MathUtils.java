/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

/**
 * Util class for tests.
 */
public final class MathUtils {
  private MathUtils() { }

  private static final long[] FACTORIALS = {
      1,                    // 0!
      1,                    // 1!
      2,                    // 2!
      6,                    // 3!
      24,                   // 4!
      120,                  // 5!
      720,                  // 6!
      5040,                 // 7!
      40320,                // 8!
      362880,               // 9!
      3628800,              // 10!
      39916800,             // 11!
      479001600,            // 12!
      6227020800L,          // 13!
      87178291200L,         // 14!
      1307674368000L,       // 15!
      20922789888000L,      // 16!
      355687428096000L,     // 17!
      6402373705728000L,    // 18!
      121645100408832000L,  // 19!
      2432902008176640000L, // 20!
  };

  public static long combinationWithoutRepetitions(int n, int k) {
    if (n < 0 || n > FACTORIALS.length) {
      throw new IllegalStateException("Combinations doesn't ready for N < 0 and N > 20, N = " + n);
    }
    if (k > n) {
      return 0;
    }
    long nominator = FACTORIALS[n];
    long denominatorK = FACTORIALS[k];
    long denominatorNMinusK = FACTORIALS[n - k];
    return nominator / (denominatorK * denominatorNMinusK);
  }
}
