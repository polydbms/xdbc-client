#include <iostream>
#include <thread>
#include "../xdbc/xclient.h"
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <boost/program_options.hpp>

#include "Tester.h"

static xdbc::SchemaAttribute createSchemaAttribute(std::string name, std::string tpe, int size) {
    xdbc::SchemaAttribute att;
    att.name = std::move(name);
    att.tpe = std::move(tpe);
    att.size = size;
    return att;
}

using namespace std;
namespace po = boost::program_options;

void handleCMDParams(int ac, char *av[], xdbc::RuntimeEnv &env) {
    // Declare the supported options.
    po::options_description desc("Usage: ./test_client [options]\n\nAllowed options");
    desc.add_options()
            ("help,h", "Produce this help message.")
            ("table,e", po::value<string>()->default_value("test_10000000"),
             "Set table: \nDefault:\n  test_10000000")
            ("server-host,a", po::value<string>()->default_value("xdbcserver"),
             "Set server host address: \nDefault:\n  xdbcserver")
            ("intermediate-format,f", po::value<int>()->default_value(1),
             "Set intermediate-format: \nDefault:\n  1 (row)\nOther:\n  2 (col)")
            ("buffer-size,b", po::value<int>()->default_value(1000),
             "Set buffer-size of buffers used to read data from the database.\nDefault: 1000")
            ("bufferpool-size,p", po::value<int>()->default_value(1000),
             "Set the amount of buffers used.\nDefault: 1000")
            ("tuple-size,t", po::value<int>()->default_value(48), "Set the tuple size.\nDefault: 48")
            ("sleep-time,s", po::value<int>()->default_value(5), "Set a sleep-time in milli seconds.\nDefault: 5ms")
            ("mode,m", po::value<int>()->default_value(1), "1: Analytics, 2: Storage.\nDefault: 1")
            ("net-parallelism,n", po::value<int>()->default_value(1), "Set the network parallelism grade.\nDefault: 1")
            ("write-parallelism,r", po::value<int>()->default_value(1), "Set the read parallelism grade.\nDefault: 1")
            ("decomp-parallelism,d", po::value<int>()->default_value(4),
             "Set the decompression parallelism grade.\nDefault: 1")
            ("transfer-id,tid", po::value<long>()->default_value(0),
             "Set the transfer id.\nDefault: 0");

    po::positional_options_description p;
    p.add("compression-type", 1);

    po::variables_map vm;
    po::store(po::command_line_parser(ac, av).options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << "\n";
        exit(0);
    }

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
    if (vm.count("write-parallelism")) {
        spdlog::get("XCLIENT")->info("Write parallelism: {0}", vm["write-parallelism"].as<int>());
        env.write_parallelism = vm["write-parallelism"].as<int>();
    }
    if (vm.count("decomp-parallelism")) {
        spdlog::get("XCLIENT")->info("Decompression parallelism: {0}", vm["decomp-parallelism"].as<int>());
        env.decomp_parallelism = vm["decomp-parallelism"].as<int>();
    }
    if (vm.count("mode")) {
        spdlog::get("XCLIENT")->info("Mode: {0}", vm["mode"].as<int>());
        env.mode = vm["mode"].as<int>();
    }
    if (vm.count("transfer-id")) {
        spdlog::get("XCLIENT")->info("Transfer id: {0}", vm["transfer-id"].as<long>());
        env.transfer_id = vm["transfer-id"].as<long>();
    }
    if (vm.count("server-host")) {
        spdlog::get("XCLIENT")->info("Server host: {0}", vm["server-host"].as<string>());
        env.server_host = vm["server-host"].as<string>();
    }


    //env.server_host = "xdbcserver";
    env.server_port = "1234";

    env.rcv_time = 0;
    env.decomp_time = 0;
    env.write_time = 0;

    env.rcv_wait_time = 0;
    env.decomp_wait_time = 0;
    env.write_wait_time = 0;

}

int main(int argc, char *argv[]) {

    auto console = spdlog::stdout_color_mt("XCLIENT");

    xdbc::RuntimeEnv env;
    handleCMDParams(argc, argv, env);
    env.env_name = "Cpp Client";

    //create schema
    std::vector<xdbc::SchemaAttribute> schema;

    if(env.table.find("lineitem") != std::string::npos) {
        schema.emplace_back(createSchemaAttribute("l_orderkey", "INT", 4));
        schema.emplace_back(createSchemaAttribute("l_partkey", "INT", 4));
        schema.emplace_back(createSchemaAttribute("l_suppkey", "INT", 4));
        schema.emplace_back(createSchemaAttribute("l_linenumber", "INT", 4));
        schema.emplace_back(createSchemaAttribute("l_quantity", "DOUBLE", 8));
        schema.emplace_back(createSchemaAttribute("l_extendedprice", "DOUBLE", 8));
        schema.emplace_back(createSchemaAttribute("l_discount", "DOUBLE", 8));
        schema.emplace_back(createSchemaAttribute("l_tax", "DOUBLE", 8));
    }

    else if (env.table.find("ss13") != std::string::npos) {
        schema.emplace_back(createSchemaAttribute("SERIALNO", "INT", 4));
        schema.emplace_back(createSchemaAttribute("DIVISION", "INT", 4));
        schema.emplace_back(createSchemaAttribute("PUMA", "INT", 4));
        schema.emplace_back(createSchemaAttribute("REGION", "INT", 4));
        schema.emplace_back(createSchemaAttribute("ST", "INT", 4));
        schema.emplace_back(createSchemaAttribute("ADJHSG", "INT", 4));
        schema.emplace_back(createSchemaAttribute("ADJINC", "INT", 4));
        schema.emplace_back(createSchemaAttribute("WGTP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("NP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("TYPE", "INT", 4));
        schema.emplace_back(createSchemaAttribute("ACCESS", "INT", 4));
        schema.emplace_back(createSchemaAttribute("ACR", "INT", 4));
        schema.emplace_back(createSchemaAttribute("AGS", "INT", 4));
        schema.emplace_back(createSchemaAttribute("BATH", "INT", 4));
        schema.emplace_back(createSchemaAttribute("BDSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("BLD", "INT", 4));
        schema.emplace_back(createSchemaAttribute("BROADBND", "INT", 4));
        schema.emplace_back(createSchemaAttribute("BUS", "INT", 4));
        schema.emplace_back(createSchemaAttribute("COMPOTHX", "INT", 4));
        schema.emplace_back(createSchemaAttribute("CONP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("DIALUP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("DSL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("ELEP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FIBEROP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FS", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FULP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("GASP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HANDHELD", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HFL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("INSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("LAPTOP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("MHP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("MODEM", "INT", 4));
        schema.emplace_back(createSchemaAttribute("MRGI", "INT", 4));
        schema.emplace_back(createSchemaAttribute("MRGP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("MRGT", "INT", 4));
        schema.emplace_back(createSchemaAttribute("MRGX", "INT", 4));
        schema.emplace_back(createSchemaAttribute("OTHSVCEX", "INT", 4));
        schema.emplace_back(createSchemaAttribute("REFR", "INT", 4));
        schema.emplace_back(createSchemaAttribute("RMSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("RNTM", "INT", 4));
        schema.emplace_back(createSchemaAttribute("RNTP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("RWAT", "INT", 4));
        schema.emplace_back(createSchemaAttribute("RWATPR", "INT", 4));
        schema.emplace_back(createSchemaAttribute("SATELLITE", "INT", 4));
        schema.emplace_back(createSchemaAttribute("SINK", "INT", 4));
        schema.emplace_back(createSchemaAttribute("SMP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("STOV", "INT", 4));
        schema.emplace_back(createSchemaAttribute("TEL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("TEN", "INT", 4));
        schema.emplace_back(createSchemaAttribute("TOIL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("VACS", "INT", 4));
        schema.emplace_back(createSchemaAttribute("VALP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("VEH", "INT", 4));
        schema.emplace_back(createSchemaAttribute("WATP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("YBL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FES", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FFINCP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FGRNTP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FHINCP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FINCP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FPARC", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FSMOCP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("GRNTP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("GRPIP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HHL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HHT", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HINCP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HUGCL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HUPAC", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HUPAOC", "INT", 4));
        schema.emplace_back(createSchemaAttribute("HUPARC", "INT", 4));
        schema.emplace_back(createSchemaAttribute("KIT", "INT", 4));
        schema.emplace_back(createSchemaAttribute("LNGI", "INT", 4));
        schema.emplace_back(createSchemaAttribute("MULTG", "INT", 4));
        schema.emplace_back(createSchemaAttribute("MV", "INT", 4));
        schema.emplace_back(createSchemaAttribute("NOC", "INT", 4));
        schema.emplace_back(createSchemaAttribute("NPF", "INT", 4));
        schema.emplace_back(createSchemaAttribute("NPP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("NR", "INT", 4));
        schema.emplace_back(createSchemaAttribute("NRC", "INT", 4));
        schema.emplace_back(createSchemaAttribute("OCPIP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("PARTNER", "INT", 4));
        schema.emplace_back(createSchemaAttribute("PLM", "INT", 4));
        schema.emplace_back(createSchemaAttribute("PSF", "INT", 4));
        schema.emplace_back(createSchemaAttribute("R18", "INT", 4));
        schema.emplace_back(createSchemaAttribute("R60", "INT", 4));
        schema.emplace_back(createSchemaAttribute("R65", "INT", 4));
        schema.emplace_back(createSchemaAttribute("RESMODE", "INT", 4));
        schema.emplace_back(createSchemaAttribute("SMOCP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("SMX", "INT", 4));
        schema.emplace_back(createSchemaAttribute("SRNT", "INT", 4));
        schema.emplace_back(createSchemaAttribute("SSMC", "INT", 4));
        schema.emplace_back(createSchemaAttribute("SVAL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("TAXP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("WIF", "INT", 4));
        schema.emplace_back(createSchemaAttribute("WKEXREL", "INT", 4));
        schema.emplace_back(createSchemaAttribute("WORKSTAT", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FACCESSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FACRP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FAGSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FBATHP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FBDSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FBLDP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FBROADBNDP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FBUSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FCOMPOTHXP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FCONP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FDIALUPP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FDSLP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FELEP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FFIBEROPP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FFSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FFULP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FGASP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FHANDHELDP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FHFLP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FINSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FKITP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FLAPTOPP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FMHP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FMODEMP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FMRGIP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FMRGP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FMRGTP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FMRGXP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FMVP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FOTHSVCEXP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FPLMP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FREFRP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FRMSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FRNTMP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FRNTP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FRWATP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FRWATPRP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FSATELLITEP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FSINKP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FSMP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FSMXHP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FSMXSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FSTOVP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FTAXP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FTELP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FTENP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FTOILP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FVACSP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FVALP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FVEHP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FWATP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("FYBLP", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp1", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp2", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp3", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp4", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp5", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp6", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp7", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp8", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp9", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp10", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp11", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp12", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp13", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp14", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp15", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp16", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp17", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp18", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp19", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp20", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp21", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp22", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp23", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp24", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp25", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp26", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp27", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp28", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp29", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp30", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp31", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp32", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp33", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp34", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp35", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp36", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp37", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp38", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp39", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp40", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp41", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp42", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp43", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp44", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp45", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp46", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp47", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp48", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp49", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp50", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp51", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp52", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp53", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp54", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp55", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp56", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp57", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp58", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp59", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp60", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp61", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp62", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp63", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp64", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp65", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp66", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp67", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp68", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp69", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp70", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp71", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp72", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp73", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp74", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp75", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp76", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp77", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp78", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp79", "INT", 4));
        schema.emplace_back(createSchemaAttribute("wgtp80", "INT", 4));
    }
    env.schema = schema;

    Tester tester("Cpp Client", env, schema);

    auto start = std::chrono::high_resolution_clock::now();
    if (env.mode == 1)
        tester.runAnalytics();
    else if (env.mode == 2)
        tester.runStorage("/dev/shm/output");

    auto duration_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start).count();
    env.write_time.fetch_add(duration_microseconds, std::memory_order_relaxed);

    tester.close();


    return 0;
}