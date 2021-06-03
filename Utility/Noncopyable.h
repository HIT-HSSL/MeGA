/*
 * Author   : Xiangyu Zou
 * Date     : 04/23/2021
 * Time     : 15:39
 * Project  : MeGA
 This source code is licensed under the GPLv2
 */

#ifndef MEGA_NONCOPYABLE_H
#define MEGA_NONCOPYABLE_H

class noncopyable {
protected:
    noncopyable() = default;

    ~noncopyable() = default;

private:
    noncopyable(const noncopyable &) = delete;

    const noncopyable &operator=(const noncopyable &) = delete;
};

#endif //MEGA_NONCOPYABLE_H
