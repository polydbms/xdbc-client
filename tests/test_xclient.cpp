#include <iostream>
#include <thread>
#include "../xdbc/xclient.h"
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <boost/program_options.hpp>

using namespace std;
namespace po = boost::program_options;

xdbc::RuntimeEnv handleCMDParams(int ac, char *av[]) {
    // Declare the supported options.
    po::options_description desc("Usage: ./test_client [options]\n\nAllowed options");
    desc.add_options()
            ("help,h", "Produce this help message.")
            ("table,e", po::value<string>()->default_value("test_10000000"),
             "Set table: \nDefault:\n  test_10000000")
            ("intermediate-format,f", po::value<int>()->default_value(1),
             "Set intermediate-format: \nDefault:\n  1 (row)\nOther:\n  2 (col)")
            ("buffer-size,b", po::value<int>()->default_value(1000),
             "Set buffer-size of buffers used to read data from the database.\nDefault: 1000")
            ("bufferpool-size,p", po::value<int>()->default_value(1000),
             "Set the amount of buffers used.\nDefault: 1000")
            ("tuple-size,t", po::value<int>()->default_value(48), "Set the tuple size.\nDefault: 48")
            ("sleep-time,s", po::value<int>()->default_value(5), "Set a sleep-time in milli seconds.\nDefault: 5ms")
            ("parallelism,P", po::value<int>()->default_value(4), "Set the parallelism grade.\nDefault: 4");

    po::positional_options_description p;
    p.add("compression-type", 1);

    po::variables_map vm;
    po::store(po::command_line_parser(ac, av).options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << "\n";
        exit(0);
    }

    xdbc::RuntimeEnv env;

    if (vm.count("table")) {
        spdlog::get("XCLIENT")->info("Table: {0}", vm["table"].as<string>());
        env.table = vm["table"].as<string>();
    }
    if (vm.count("intermediate-format")) {
        spdlog::get("XCLIENT")->info("Intermediate format: {0}", vm["intermediate-format"].as<int>());
        env.iformat = vm["intermediate-format"].as<int>();
    }

    if (vm.count("buffer-size")) {
        spdlog::get("XCLIENT")->info("Buffer size: {0}", vm["buffer-size"].as<int>());
        env.buffer_size = vm["buffer-size"].as<int>();
    }
    if (vm.count("bufferpool-size")) {
        spdlog::get("XCLIENT")->info("Bufferpool size: {0}", vm["bufferpool-size"].as<int>());
        env.bufferpool_size = vm["bufferpool-size"].as<int>();
    }
    if (vm.count("tuple-size")) {
        spdlog::get("XCLIENT")->info("Tuple size: {0}", vm["tuple-size"].as<int>());
        env.tuple_size = vm["tuple-size"].as<int>();
    }
    if (vm.count("sleep-time")) {
        spdlog::get("XCLIENT")->info("Sleep time: {0} ms", vm["sleep-time"].as<int>());
        env.sleep_time = std::chrono::milliseconds(vm["sleep-time"].as<int>());
    }
    if (vm.count("parallelism")) {
        spdlog::get("XCLIENT")->info("Parallelism: {0}", vm["parallelism"].as<int>());
        env.parallelism = vm["parallelism"].as<int>();
    }

    return env;
}

int main(int argc, char *argv[]) {

    auto console = spdlog::stdout_color_mt("XCLIENT");

    xdbc::RuntimeEnv env = handleCMDParams(argc, argv);
    env.env_name = "Cpp Client";

    //create schema
    std::vector<std::tuple<std::string, std::string, int>> schema;
    schema.emplace_back("l_orderkey", "INT", 4);
    schema.emplace_back("l_partkey", "INT", 4);
    schema.emplace_back("l_suppkey", "INT", 4);
    schema.emplace_back("l_linenumber", "INT", 4);
    schema.emplace_back("l_quantity", "DOUBLE", 8);
    schema.emplace_back("l_extendedprice", "DOUBLE", 8);
    schema.emplace_back("l_discount", "DOUBLE", 8);
    schema.emplace_back("l_tax", "DOUBLE", 8);


    env.schema = schema;

    xdbc::XClient c(env);

    spdlog::get("XCLIENT")->info("#1 Constructed XClient called: {0}", c.get_name());

    c.startReceiving(env.table);

    int min = INT32_MAX;
    int max = INT32_MIN;
    long sum = 0;
    long cnt = 0;
    long totalcnt = 0;

    spdlog::get("XCLIENT")->info("#4 called receive");

    auto start = std::chrono::steady_clock::now();

    int buffsRead = 0;
    while (c.hasUnread()) {
        xdbc::buffWithId curBuffWithId = c.getBuffer();
        //cout << "Iteration at tuple:" << cnt << " and buffer " << buffsRead << endl;
        if (curBuffWithId.id >= 0) {
            if (curBuffWithId.iformat == 1) {
                auto *ptr = reinterpret_cast<xdbc::shortLineitem *>(curBuffWithId.buff.data());
                std::vector<xdbc::shortLineitem> sls(ptr, ptr + env.buffer_size);
                for (auto sl: sls) {
                    totalcnt++;
                    //cout << "Buffer with Id: " << curBuffWithId.id << " l_orderkey: " << sl.l_orderkey << endl;
                    if (sl.l_orderkey < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}, tuple: [{2}]",
                                                     curBuffWithId.id, cnt, c.slStr(&sl));

                        break;
                    } else {
                        cnt++;
                        sum += sl.l_orderkey;
                        if (sl.l_orderkey < min)
                            min = sl.l_orderkey;
                        if (sl.l_orderkey > max)
                            max = sl.l_orderkey;

                    }
                    if (buffsRead == 1) {

                        /*spdlog::get("XCLIENT")->info(
                                "first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                                sl.l_orderkey, sl.l_partkey, sl.l_suppkey, sl.l_linenumber, sl.l_quantity,
                                sl.l_extendedprice, sl.l_discount, sl.l_tax);*/
                    }
                }

            }
            if (curBuffWithId.iformat == 2) {
                // Create a byte pointer to the starting address of the vector
                std::byte *dataPtr = curBuffWithId.buff.data();

                // Construct the first four vectors of type int at the dataPtr address


                int *v1 = reinterpret_cast<int *>(curBuffWithId.buff.data());
                int *v2 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env.buffer_size * 4);
                int *v3 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env.buffer_size * 4 * 2);
                int *v4 = reinterpret_cast<int *>(curBuffWithId.buff.data() + env.buffer_size * 4 * 3);
                double *v5 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env.buffer_size * 4 * 4);
                double *v6 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env.buffer_size * 4 * 4 +
                                                        env.buffer_size * 8 * 1);
                double *v7 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env.buffer_size * 4 * 4 +
                                                        env.buffer_size * 8 * 2);
                double *v8 = reinterpret_cast<double *>(curBuffWithId.buff.data() + env.buffer_size * 4 * 4 +
                                                        env.buffer_size * 8 * 3);

                if (buffsRead == 1) {

                    spdlog::get("XCLIENT")->info("first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                                                 v1[0], v2[0], v3[0], v4[0], v5[0], v6[0], v7[0], v8[0]);
                }

                for (int i = 0; i < env.buffer_size; i++) {
                    totalcnt++;
                    /*if (v1[i] > 0) {
                        spdlog::get("XCLIENT")->info(
                                "first shortLineitem: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} ",
                                v1[i], v2[i], v3[i], v4[i], v5[i], v6[i], v7[i], v8[i]);
                    }*/
                    //cout << "Buffer with Id: " << curBuffWithId.id << " l_orderkey: " << sl.l_orderkey << endl;
                    if (v1[i] < 0) {
                        //spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tuple_no: {1}, l_orderkey: {2}",
                        //                            curBuffWithId.id, cnt, v1[i]);
                        //c.printSl(&sl);
                        break;
                    } else {
                        cnt++;
                        sum += v1[i];
                        if (v1[i] < min)
                            min = v1[i];
                        if (v1[i] > max)
                            max = v1[i];

                    }
                }

            }
            buffsRead++;
            c.markBufferAsRead(curBuffWithId.id);
        } else {
            spdlog::get("XCLIENT")->warn("found invalid buffer with id: {0}, buff_no: {1}",
                                         curBuffWithId.id, buffsRead);
            break;
        }

    }
    c.finalize();

    spdlog::get("XCLIENT")->info("Total read buffers: {0}", buffsRead);

    auto end = std::chrono::steady_clock::now();

    spdlog::get("XCLIENT")->info("totalcnt: {0}", totalcnt);
    spdlog::get("XCLIENT")->info("cnt: {0}", cnt);
    spdlog::get("XCLIENT")->info("min: {0}", min);
    spdlog::get("XCLIENT")->info("max: {0}", max);
    spdlog::get("XCLIENT")->info("avg: {0}", (sum / (double) cnt));

    spdlog::get("XCLIENT")->info("Total elapsed time: {0} ms, #tuples: {1}",
                                 std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), cnt);


    return 0;
}