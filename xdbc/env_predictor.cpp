//
// Created by joel on 03.12.23.
//

#include "env_predictor.h"

namespace xdbc {

    ClientEnvPredictor::ClientEnvPredictor() {
        maxThreads = std::thread::hardware_concurrency();
        if (maxThreads == 0) maxThreads = 8;
    }

    /**
     * Calculates wait to active time ratios and uses that information to tweak runtime parameters for next query.
     * @param pastEnv RuntimeEnv of past query with thread times.
     * @return Tweaked RuntimeEnv for usage in next Query.
     */
    ClientRuntimeParams ClientEnvPredictor::tweakNextParams(RuntimeEnv *pastEnv) {
        ClientRuntimeParams nextParams;
        nextParams.rcv_parallelism = changeByCalculatedRatio<>(pastEnv->rcv_time.load(), pastEnv->rcv_wait_time.load(),
                                                               pastEnv->rcv_parallelism, 0.8);
        nextParams.decomp_parallelism = changeByCalculatedRatio<>(pastEnv->decomp_time.load(),
                                                                  pastEnv->decomp_wait_time.load(),
                                                                  pastEnv->decomp_parallelism, 0.8);
        nextParams.read_parallelism = changeByCalculatedRatio<>(pastEnv->write_time.load(),
                                                                pastEnv->write_wait_time.load(),
                                                                pastEnv->read_parallelism, 0.8);
        nextParams.bufferpool_size = pastEnv->bufferpool_size;
        nextParams.buffer_size = pastEnv->buffer_size;
        return nextParams;
    }

    /**
     * Change provided valToChange by the ratio of part / ( part + otherPart ) if that ratio is below ratioForIncrease. Otherwise double the valToChange.
     * @tparam T1 Numeric type for division
     * @tparam T2 Numeric type for division
     * @param part first part of the ratio calculation
     * @param otherPart second part of the ratio calculation
     * @param valToChange int value that gets changed and returned
     * @param ratioForIncrease the ratio over which the valToChange gets doubled instead of multiplied by the ratio
     * @return value changed by the ratio of part to part+otherpart
     */
    template<typename T>
    int ClientEnvPredictor::changeByCalculatedRatio(T part, T otherPart, int valToChange, float ratioForIncrease) {
        double ratio = static_cast<float>(part) / (part + otherPart);
        if (ratio > ratioForIncrease) return std::min(maxThreads, valToChange * 2);
        else return std::max(1, static_cast<int>(std::ceil(valToChange * ratio)));
    }
}