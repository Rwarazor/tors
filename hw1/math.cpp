#include "math.h"

#include <cmath>
#include <numbers>
#include <vector>

namespace hw1 {

namespace math {

double FunctionAt(double x) {
    return std::pow(std::abs(std::sin(x)), std::numbers::e) + std::cos(sqrt(std::abs(x)));
}

std::size_t hash(std::vector<std::size_t> vec) {
    std::size_t ans = 0;
    for (auto elem : vec) {
        ans ^= std::hash<std::size_t>()(vec[0] + std::hash<std::size_t>()(ans));
    }
    return ans;
}

} // namespace math

} // namespace hw1