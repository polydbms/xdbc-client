#include "CSVSink/CSVSink.h"
#include "PQSink/PQSink.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>
#include "../xdbc/xclient.h"
#include <iostream>
#include <thread>

// Utility functions for schema handling
static xdbc::SchemaAttribute createSchemaAttribute(std::string name, std::string tpe, int size) {
    xdbc::SchemaAttribute att;
    att.name = std::move(name);
    att.tpe = std::move(tpe);
    att.size = size;
    return att;
}

std::string formatSchema(const std::vector<xdbc::SchemaAttribute> &schema) {
    std::stringstream ss;
    ss << std::setw(20) << std::left << "Name"
       << std::setw(15) << std::left << "Type"
       << std::setw(10) << std::left << "Size" << '\n';

    for (const auto &tuple: schema) {
        ss << std::setw(20) << std::left << tuple.name
           << std::setw(15) << std::left << tuple.tpe
           << std::setw(10) << std::left << tuple.size << '\n';
    }
    return ss.str();
}

std::vector<xdbc::SchemaAttribute> createSchemaFromConfig(const std::string &configFile) {
    std::ifstream file(configFile);
    if (!file.is_open()) {
        spdlog::get("XDBC.SINK")->error("Failed to open schema file: {0}", configFile);
        exit(EXIT_FAILURE);
    }

    nlohmann::json schemaJson;
    file >> schemaJson;

    std::vector<xdbc::SchemaAttribute> schema;
    for (const auto &item: schemaJson) {
        schema.emplace_back(xdbc::SchemaAttribute{
                item["name"], item["type"], item["size"]});
    }
    return schema;
}

std::string readJsonFileIntoString(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        spdlog::get("XDBC.SINK")->error("Failed to open schema file: {0}", filePath);
        exit(EXIT_FAILURE);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

void handleSinkCMDParams(int argc, char *argv[], xdbc::RuntimeEnv &env, std::string &outputBasePath) {
    namespace po = boost::program_options;

    po::options_description desc("Usage: ./csvsink [options]\n\nAllowed options");
    desc.add_options()
            ("help,h", "Produce help message.")
            ("server-host,a", po::value<std::string>()->default_value("xdbcserver"),
             "Server Host: \nDefault:\n  xdbcserver")
            ("table,e", po::value<std::string>()->default_value("lineitem_sf10"), "Input table name.")
            ("output,o", po::value<std::string>()->default_value("/dev/shm/output"), "Output CSV base file path.")
            ("buffer-size,b", po::value<int>()->default_value(64), "Buffer size in KiB.")
            ("bufferpool-size,p", po::value<int>()->default_value(4096), "Buffer pool size in KiB.")
            ("net-parallelism,n", po::value<int>()->default_value(1), "Set the network parallelism grade.\nDefault: 1")
            ("decomp-parallelism,d", po::value<int>()->default_value(1), "Decompression Parallelism.\nDefault: 1")
            ("serialize-parallelism,s", po::value<int>()->default_value(1), "Number of serializer threads.")
            ("write-parallelism,w", po::value<int>()->default_value(1), "Number of write threads.")
            ("intermediate-format,f", po::value<int>()->default_value(1),
             "Intermediate format: 1 (row) or 2 (column).")
            ("transfer-id,tid", po::value<long>()->default_value(0),
             "Set the transfer id.\nDefault: 0")
            ("skip-serializer", po::value<int>()->default_value(0),
             "Skip serialization (0/1).\nDefault: false")
            ("target", po::value<std::string>()->default_value("csv"),
             "Target (csv, parquet).\nDefault: csv");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        exit(0);
    }

    try {
        po::notify(vm);
    } catch (po::required_option &e) {
        spdlog::get("XDBC.SINK")->error("Missing required options: {0}", e.what());
        exit(EXIT_FAILURE);
    }

    env.env_name = "Sink";
    env.server_host = vm["server-host"].as<std::string>();
    env.server_port = "1234";
    env.table = vm["table"].as<std::string>();
    env.buffer_size = vm["buffer-size"].as<int>();
    env.buffers_in_bufferpool = vm["bufferpool-size"].as<int>() / vm["buffer-size"].as<int>();
    env.rcv_parallelism = vm["net-parallelism"].as<int>();
    env.decomp_parallelism = vm["decomp-parallelism"].as<int>();
    env.ser_parallelism = vm["serialize-parallelism"].as<int>();
    env.write_parallelism = vm["write-parallelism"].as<int>();
    env.iformat = vm["intermediate-format"].as<int>();
    env.target = vm["target"].as<std::string>();
    outputBasePath = vm["output"].as<std::string>();

    env.skip_serializer = vm["skip-serializer"].as<int>();

    std::string schemaFile = "/xdbc-client/tests/schemas/" + env.table + ".json";

    env.schema = createSchemaFromConfig(schemaFile);
    env.schemaJSON = readJsonFileIntoString(schemaFile);
    env.tuple_size = std::accumulate(env.schema.begin(), env.schema.end(), 0,
                                     [](int acc, const xdbc::SchemaAttribute &attr) {
                                         return acc + attr.size;
                                     });

    env.tuples_per_buffer = (env.buffer_size * 1024) / env.tuple_size;
    env.startTime = std::chrono::steady_clock::now();

    spdlog::get("XDBC.SINK")->info("Table: {0}, Tuple size: {1}, Schema:\n{2}",
                                   env.table, env.tuple_size, formatSchema(env.schema));
}

int main(int argc, char *argv[]) {
    auto console = spdlog::stdout_color_mt("XDBC.SINK");
    spdlog::set_level(spdlog::level::info);

    xdbc::RuntimeEnv env;
    std::string outputBasePath;

    handleSinkCMDParams(argc, argv, env, outputBasePath);

    // Initialize XClient
    xdbc::XClient xclient(env);
    xclient.startReceiving(env.table);

    if (env.target == "csv") {
        CsvSink csvSink(outputBasePath, &env);

        // Start serialization and writing threads
        std::vector<std::thread> threads;
        for (int i = 0; i < env.ser_parallelism; ++i) {
            xclient._serThreads[i] = std::thread(&CsvSink::serialize, &csvSink, i);
        }
        for (int i = 0; i < env.write_parallelism; ++i) {
            xclient._writeThreads[i] = std::thread(&CsvSink::write, &csvSink, i);
        }
    } else if (env.target == "parquet") {
        PQSink parquetSink(outputBasePath, &env);

        // Start serialization and writing threads
        std::vector<std::thread> threads;
        for (int i = 0; i < env.ser_parallelism; ++i) {
            xclient._serThreads[i] = std::thread(&PQSink::serialize, &parquetSink, i);
        }
        for (int i = 0; i < env.write_parallelism; ++i) {
            xclient._writeThreads[i] = std::thread(&PQSink::write, &parquetSink, i);
        }
    }

    // Wait for threads to finish
    for (int i = 0; i < env.ser_parallelism; ++i) {
        xclient._serThreads[i].join();
    }
    for (int i = 0; i < env.write_parallelism; ++i) {
        xclient._writeThreads[i].join();
    }

    xclient.finalize();
    spdlog::get("XDBC.CSVSINK")->info("{} serialization completed. Output files are available at: {}",
                                      env.target, outputBasePath);

    return 0;
}
