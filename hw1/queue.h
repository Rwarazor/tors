#pragma once

#include <atomic>
#include <mutex>
#include <optional>
#include <queue>

template <typename T>
class Queue {
  public:
    Queue() {};

    void push(T item, bool lock = true) {
        if (lock) {
            mtx.lock();
        }
        queue_.push(std::move(item));
        cnt += 1;
        if (lock) {
            mtx.unlock();
        }
    }

    std::optional<T> pop(bool lock = true) {
        if (cnt == 0) {
            return std::nullopt;
        } else {
            if (lock) {
                mtx.lock();
            }
            if (cnt == 0) {
                if (lock) {
                    mtx.unlock();
                }
                return std::nullopt;
            }
            cnt--;
            auto elem = queue_.front();
            queue_.pop();
            if (lock) {
                mtx.unlock();
            }
            return elem;
        }
    }

    std::mutex mtx;

  private:
    std::atomic_int cnt;
    std::queue<T> queue_;
};
