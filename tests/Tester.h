#include "../xdbc/xclient.h"
#include <string>

class Tester {

public:

    Tester(std::string name, xdbc::RuntimeEnv &env,
           std::vector<std::tuple<std::string, std::string, int>> schema);

    void runAnalytics();

    void runStorage(const std::string &filename);

    void close();

private:
    xdbc::RuntimeEnv *env;
    std::vector<std::tuple<std::string, std::string, int>> schema;
    xdbc::XClient xclient;
    std::string name;
    std::chrono::steady_clock::time_point start;

    int analyticsThread(int thr, int &mins, int &maxs, long &sums, long &cnts, long &totalctns);

    int storageThread(int thr, std::ofstream& csvFile);


};