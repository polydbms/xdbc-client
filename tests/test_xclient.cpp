#include <iostream>
#include <thread>
#include "../xdbc/xclient.h"
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <boost/program_options.hpp>

#include "Tester.h"

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
            ("mode,m", po::value<int>()->default_value(1), "1: Analytics, 2: Storage.\nDefault: 1")
            ("net-parallelism,n", po::value<int>()->default_value(4), "Set the network parallelism grade.\nDefault: 1")
            ("read-parallelism,r", po::value<int>()->default_value(4), "Set the read parallelism grade.\nDefault: 1")
            ("decomp-parallelism,d", po::value<int>()->default_value(4),
             "Set the decompression parallelism grade.\nDefault: 1");

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
    if (vm.count("net-parallelism")) {
        spdlog::get("XCLIENT")->info("Network parallelism: {0}", vm["net-parallelism"].as<int>());
        env.rcv_parallelism = vm["net-parallelism"].as<int>();
    }
    if (vm.count("read-parallelism")) {
        spdlog::get("XCLIENT")->info("Read parallelism: {0}", vm["read-parallelism"].as<int>());
        env.read_parallelism = vm["read-parallelism"].as<int>();
    }
    if (vm.count("decomp-parallelism")) {
        spdlog::get("XCLIENT")->info("Decompression parallelism: {0}", vm["decomp-parallelism"].as<int>());
        env.decomp_parallelism = vm["decomp-parallelism"].as<int>();
    }
    if (vm.count("mode")) {
        spdlog::get("XCLIENT")->info("Mode: {0}", vm["mode"].as<int>());
        env.mode = vm["mode"].as<int>();
    }


    env.server_host = "xdbcserver";
    env.server_port = "1234";

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

    Tester tester("Cpp Client", env, schema);

    if (env.mode == 1)
        tester.runAnalytics();
    else if (env.mode == 2)
        tester.runStorage("/dev/shm/output.csv");

    tester.close();


    return 0;
}