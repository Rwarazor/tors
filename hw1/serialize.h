#pragma once

#include "proto.h"

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

namespace hw1 {

namespace serialize {

template <typename T>
std::string ToBytes(const T &obj);

template <typename T>
void FromBytes(std::istream &in, T &res);

} // namespace serialize

} // namespace hw1
