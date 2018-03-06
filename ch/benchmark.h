#pragma once

#include <chrono>

using namespace std;

#define MIL(x) ((x) / 1000000.0)
#define SEC(x) (MIL(x) / 1000.0)
#define PRINT_SEC(name, x) cout << (name) << ": " << fixed << SEC(x) << " sec" << endl;
#define PRINT_MIL(name, x) cout << (name) << ": " << fixed << MIL(x) << " millisec" << endl;

class Timer {
public:
    Timer() {
        start = chrono::steady_clock::now();
    }

    long stop() {
        auto end = chrono::steady_clock::now();
        return (end - start).count();
    }

    void restart() {
        start = chrono::steady_clock::now();
    }
private:
    chrono::time_point<chrono::steady_clock> start;
};


