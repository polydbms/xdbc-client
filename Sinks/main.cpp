#include "CSVSink/CSVSink.h"
#include "PQSink/PQSink.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>
#include "../xdbc/xclient.h"
#include <iostream>
#include <thread>
#include "../xdbc/ControllerInterface/WebSocketClient.h"
#include "../xdbc/metrics_calculator.h"

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
    desc.add_options()("help,h", "Produce help message.")("server-host,a",
                                                          po::value<std::string>()->default_value("xdbcserver"),
                                                          "Server Host: \nDefault:\n  xdbcserver")("server-port",
                                                                                                   po::value<std::string>()->default_value(
                                                                                                           "1234"),
                                                                                                   "Server port: \nDefault:\n  1234")(
            "table,e", po::value<std::string>()->default_value("lineitem_sf10"), "Input table name.")("output,o",
                                                                                                      po::value<std::string>()->default_value(
                                                                                                              "/dev/shm/output"),
                                                                                                      "Output CSV base file path.")(
            "buffer-size,b", po::value<int>()->default_value(64), "Buffer size in KiB.")("bufferpool-size,p",
                                                                                         po::value<int>()->default_value(
                                                                                                 4096),
                                                                                         "Buffer pool size in KiB.")(
            "net-parallelism,n", po::value<int>()->default_value(1), "Set the network parallelism grade.\nDefault: 1")(
            "decomp-parallelism,d", po::value<int>()->default_value(1), "Decompression Parallelism.\nDefault: 1")(
            "serialize-parallelism,s", po::value<int>()->default_value(1), "Number of serializer threads.")(
            "write-parallelism,w", po::value<int>()->default_value(1), "Number of write threads.")(
            "intermediate-format,f", po::value<int>()->default_value(1),
            "Intermediate format: 1 (row) or 2 (column).")("transfer-id,tid", po::value<long>()->default_value(0),
                                                           "Set the transfer id.\nDefault: 0")("profiling-interval",
                                                                                               po::value<int>()->default_value(
                                                                                                       1000),
                                                                                               "Set profiling interval.\nDefault: 1000")(
            "skip-serializer", po::value<int>()->default_value(0),
            "Skip serialization (0/1).\nDefault: false")("target", po::value<std::string>()->default_value("csv"),
                                                         "Target (csv, parquet).\nDefault: csv")("spawn-source",
                                                                                                 po::value<int>()->default_value(
                                                                                                         0),
                                                                                                 "Set spawn source (0 means direct launch or 1 means spawned using controller).\nDefault: 0");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        exit(0);
    }

    try {
        po::notify(vm);
    }
    catch (po::required_option &e) {
        spdlog::get("XDBC.SINK")->error("Missing required options: {0}", e.what());
        exit(EXIT_FAILURE);
    }

    env.env_name = "Sink";
    env.server_host = vm["server-host"].as<std::string>();
    env.server_port = vm["server-port"].as<std::string>();
    env.transfer_id = vm["transfer-id"].as<long>();
    env.table = vm["table"].as<std::string>();
    env.buffer_size = vm["buffer-size"].as<int>();
    env.buffers_in_bufferpool = vm["bufferpool-size"].as<int>() / vm["buffer-size"].as<int>();
    env.rcv_parallelism = vm["net-parallelism"].as<int>();
    env.decomp_parallelism = vm["decomp-parallelism"].as<int>();
    env.ser_parallelism = vm["serialize-parallelism"].as<int>();
    env.write_parallelism = vm["write-parallelism"].as<int>();
    env.iformat = vm["intermediate-format"].as<int>();
    env.target = vm["target"].as<std::string>();
    env.profilingInterval = vm["profiling-interval"].as<int>();
    outputBasePath = vm["output"].as<std::string>();

    env.skip_serializer = vm["skip-serializer"].as<int>();
    env.spawn_source = vm["spawn-source"].as<int>();

    std::string schemaFile = "/xdbc-client/tests/schemas/" + env.table + ".json";

    env.schema = createSchemaFromConfig(schemaFile);
    env.schemaJSON = readJsonFileIntoString(schemaFile);
    env.tuple_size = std::accumulate(env.schema.begin(), env.schema.end(), 0,
                                     [](int acc, const xdbc::SchemaAttribute &attr) {
                                         return acc + attr.size;
                                     });

    env.tuples_per_buffer = (env.buffer_size * 1024) / env.tuple_size;
    env.max_threads = env.buffers_in_bufferpool;
    env.startTime = std::chrono::steady_clock::now();

    spdlog::get("XDBC.SINK")->info("Table: {0}, Tuple size: {1}, Schema:\n{2}", env.table, env.tuple_size,
                                   formatSchema(env.schema));
}

nlohmann::json metrics_convert(xdbc::RuntimeEnv &env) {
    nlohmann::json metrics_json = nlohmann::json::object(); // Use a JSON object
    if ((env.pts) && (env.enable_updation == 1)) {
        std::vector<xdbc::ProfilingTimestamps> env_pts;
        env_pts = env.pts->copy_newElements();
        auto component_metrics_ = calculate_metrics(env_pts, env.buffer_size);
        for (const auto &pair: component_metrics_) {
            nlohmann::json metric_object = nlohmann::json::object();
            const Metrics &metric = pair.second;

            metric_object["waitingTime_ms"] = metric.waiting_time_ms;
            metric_object["processingTime_ms"] = metric.processing_time_ms;
            metric_object["totalTime_ms"] = metric.overall_time_ms;

            metric_object["totalThroughput"] = metric.total_throughput;
            metric_object["perBufferThroughput"] = metric.per_buffer_throughput;

            metrics_json[pair.first] = metric_object;
        }
    }
    return metrics_json;
}

nlohmann::json additional_msg(xdbc::RuntimeEnv &env) {
    nlohmann::json metrics_json = nlohmann::json::object(); // Use a JSON object
    metrics_json["totalTime_ms"] = env.tf_paras.elapsed_time;
    metrics_json["bufTransferred"] = std::accumulate(env.tf_paras.bufProcessed.begin(), env.tf_paras.bufProcessed.end(),
                                                     0);

    if (env.enable_updation == 1) {
        metrics_json["freeBufferQ_load"] = std::get<0>(env.tf_paras.latest_queueSizes);
        metrics_json["compressedBufferQ_load"] = std::get<1>(env.tf_paras.latest_queueSizes);
        metrics_json["decompressedBufferQ_load"] = std::get<2>(env.tf_paras.latest_queueSizes);
        metrics_json["serializedBufferQ_load"] = std::get<3>(env.tf_paras.latest_queueSizes);
    }
    return metrics_json;
}

void env_convert(xdbc::RuntimeEnv &env, const nlohmann::json &env_json) {
    try {
        // env.buffer_size = std::stoi(env_json.at("bufferSize").get<std::string>());
        // env.buffers_in_bufferpool = std::stoi(env_json.at("bufferpoolSize").get<std::string>()) / env_.buffer_size;
        // env.rcv_parallelism = std::stoi(env_json.at("netParallelism").get<std::string>());

        // Update the actual environment object if updates are allowed
        if (env.enable_updation == 1) {
            env.write_parallelism = std::stoi(env_json.at("writeParallelism").get<std::string>());
            env.decomp_parallelism = std::stoi(env_json.at("decompParallelism").get<std::string>());
            env.ser_parallelism = std::stoi(env_json.at("serParallelism").get<std::string>());
            env.env_manager.configureThreads("write", env.write_parallelism);
            env.env_manager.configureThreads("serial", env.ser_parallelism);
            env.env_manager.configureThreads("decompress", env.decomp_parallelism);
        }
    }
    catch (const std::exception &e) {
        std::cerr << "Error converting env JSON: " << e.what() << std::endl;
    }
}

int main(int argc, char *argv[]) {
    auto console = spdlog::stdout_color_mt("XDBC.SINK");
    spdlog::set_level(spdlog::level::info);

    xdbc::RuntimeEnv env;
    std::string outputBasePath;

    handleSinkCMDParams(argc, argv, env, outputBasePath);

    //***
    // Initialize XClient
    xdbc::XClient xclient(env);
    xclient.startReceiving(env.table);

    if (env.target == "csv") {
        CsvSink csvSink(outputBasePath, &env);

        env.env_manager.registerOperation("serial", [&](int thr) {
            try {
                if (thr >= env.max_threads) {
                    spdlog::get("XCLIENT")->error("No of threads exceed limit");
                    return;
                }
                csvSink.serialize(thr);
            } catch (const std::exception &e) {
                spdlog::get("XCLIENT")->error("Exception in thread {}: {}", thr, e.what());
            } catch (...) {
                spdlog::get("XCLIENT")->error("Unknown exception in thread {}", thr);
            }
        }, env.decompressedBufferIds);

        env.env_manager.registerOperation("write", [&](int thr) {
            try {
                if (thr >= env.max_threads) {
                    spdlog::get("XCLIENT")->error("No of threads exceed limit");
                    return;
                }
                csvSink.write(thr);
            } catch (const std::exception &e) {
                spdlog::get("XCLIENT")->error("Exception in thread {}: {}", thr, e.what());
            } catch (...) {
                spdlog::get("XCLIENT")->error("Unknown exception in thread {}", thr);
            }
        }, env.serializedBufferIds);

        env.env_manager.configureThreads("serial", env.ser_parallelism);
        env.env_manager.configureThreads("write", env.write_parallelism);
    } else if (env.target == "parquet") {
        PQSink parquetSink(outputBasePath, &env);

        env.env_manager.registerOperation("serial", [&](int thr) {
            try {
                if (thr >= env.max_threads) {
                    spdlog::get("XCLIENT")->error("No of threads exceed limit");
                    return;
                }
                parquetSink.serialize(thr);
            } catch (const std::exception &e) {
                spdlog::get("XCLIENT")->error("Exception in thread {}: {}", thr, e.what());
            } catch (...) {
                spdlog::get("XCLIENT")->error("Unknown exception in thread {}", thr);
            }
        }, env.decompressedBufferIds);

        env.env_manager.registerOperation("write", [&](int thr) {
            try {
                if (thr >= env.max_threads) {
                    spdlog::get("XCLIENT")->error("No of threads exceed limit");
                    return;
                }
                parquetSink.write(thr);
            } catch (const std::exception &e) {
                spdlog::get("XCLIENT")->error("Exception in thread {}: {}", thr, e.what());
            } catch (...) {
                spdlog::get("XCLIENT")->error("Unknown exception in thread {}", thr);
            }
        }, env.serializedBufferIds);

        env.env_manager.configureThreads("serial", env.ser_parallelism);  // start serial component threads
        env.env_manager.configureThreads("write", env.write_parallelism); // start write component threads
    }

    // *** Setup websocket interface for controller ***
    std::thread io_thread;
    WebSocketClient ws_client("xdbc-controller", "8002");
    if (env.spawn_source == 1) {
        env.enable_updation = 1;
        ws_client.start();
        io_thread = std::thread([&]() {
            ws_client.run(
                    std::bind(&metrics_convert, std::ref(env)),
                    std::bind(&additional_msg, std::ref(env)),
                    std::bind(&env_convert, std::ref(env), std::placeholders::_1));
        });
    }
    while (env.enable_updation == 1) // Reconfigure threads as long as it is allowed
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    // *** Finished Setup websocket interface for controller ***

    // Wait for receive threads to finish, then kill the remaining components in proper sequence : decompress-serial-write
    xclient.finishReceiving();
    env.env_manager.configureThreads("serial", 0);
    env.env_manager.joinThreads("serial");
    env.env_manager.configureThreads("write", 0);
    env.env_manager.joinThreads("write");

    xclient.finalize();
    spdlog::get("XDBC.CSVSINK")->info("{} serialization completed. Output files are available at: {}", env.target,
                                      outputBasePath);
    // *** Stop websocket client
    if (env.spawn_source == 1) {
        ws_client.stop();
        if (io_thread.joinable()) {
            io_thread.join();
        }
    }

    return 0;
}
