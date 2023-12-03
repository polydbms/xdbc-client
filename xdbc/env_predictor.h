//
// Created by joel on 03.12.23.
//
// This class shall provide Runtime Environments for the xdbc client based on past Environments.
//

#ifndef XDBC_CLIENT_ENV_PREDICTOR_H
#define XDBC_CLIENT_ENV_PREDICTOR_H

#include "xclient.h"
#include <thread>

namespace xdbc {
    struct RuntimeEnv;

    struct ClientRuntimeParams{
        int rcv_parallelism;
        int decomp_parallelism;
        int read_parallelism;
        int bufferpool_size;
        int buffer_size;
    };

    class ClientEnvPredictor {
    public:
        ClientEnvPredictor();
        ClientRuntimeParams tweakNextParams(RuntimeEnv *pastEnv);
    private:
        int maxThreads;

        template <typename T>
        int changeByCalculatedRatio(T part, T otherPart, int valToChange, float ratioForIncrease);
    };
}

#endif //XDBC_CLIENT_ENV_PREDICTOR_H
