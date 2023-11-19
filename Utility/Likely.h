/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_LIKELY_H
#define MEGA_LIKELY_H

bool likely(bool input) {
  return __builtin_expect(input, 1);
}

bool unlikely(bool input) {
  return __builtin_expect(input, 0);
}

#endif //MEGA_LIKELY_H
