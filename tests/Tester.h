#include "../xdbc/xclient.h"
#include <string>

class Tester {

public:

    Tester(std::string name, xdbc::RuntimeEnv &env);
/* 
Tester::Tester(std::string name, xdbc::RuntimeEnv &env):
    Purpose: Constructor for the `Tester` class that initializes the tester with a name and runtime environment.
    Input: `name` (std::string): The name of the tester.
           `env` (xdbc::RuntimeEnv &): A reference to the runtime environment that contains configuration details.
    Output: Initializes the `Tester` object and starts the receiving process.
    Data Processing: The constructor initializes the `name`, `env`, and `xclient` (with the environment) members, then starts the receiving process in `xclient` using `startReceiving()`. Logging is done to track the initialization.
*/
// Calls:
// - spdlog::get("XCLIENT")->info - Logs constructor initialization message with client name
// - xclient.startReceiving - Starts receiving data with given table information
// - spdlog::get("XCLIENT")->info - Logs start of receiving process


    void runAnalytics();
/* 
Tester::runAnalytics():
    Purpose: Runs analytics across multiple threads to compute min, max, sum, count, and average.
    Input: None.
    Output: Logs the total count, count, min, max, and average values of the processed data.
    Data Processing: Starts multiple threads, each running `analyticsThread`. After threads complete, it aggregates the results (min, max, sum, count) and logs the final statistics.
*/
// Calls:
// - std::thread - Creates threads for analytics processing
// - std::min_element, std::max_element, std::accumulate - Aggregates results across threads
// - spdlog::get("XCLIENT")->info - Logs results


    void runStorage(const std::string &filename);
/* 
Tester::runStorage(const std::string &filename):
    Purpose: Runs the storage process across multiple threads to write processed data to CSV files.
    Input: `filename` (const std::string &): The base filename for the output CSV files.
    Output: None (The data is written to the files).
    Data Processing: Starts multiple threads, each running `storageThread`. After threads complete, all buffers are written to CSV files. Each thread handles a separate file, and the `filename` is modified with the thread number.
*/
// - std::thread::thread() - Creates a new thread to run the storageThread function in parallel
// - std::ref() - Passes the filename by reference to the storageThread function
// - std::thread::join() - Blocks the calling thread until the storageThread completes execution for each thread


    void close();
/* 
Tester::close():
    Purpose: Closes the `Tester` by finalizing the `xclient`.
    Input: None.
    Output: Finalizes the `xclient`, ending its processes.
    Data Processing: Calls `xclient.finalize()` to close any ongoing processes or resources used by `xclient`.
*/
// Calls:
// - xclient.finalize - Finalizes the client connection or data handling



private:
    xdbc::RuntimeEnv *env;
    xdbc::XClient xclient;
    std::string name;


    int analyticsThread(int thr, int &mins, int &maxs, long &sums, long &cnts, long &totalctns);
/* 
Tester::analyticsThread(int thr, int &min, int &max, long &sum, long &cnt, long &totalcnt):
    Purpose: Analyzes data in a specific thread, calculating statistics such as min, max, sum, and counts.
    Input: `thr` (int): The thread number (for logging).
           `min` (int &), `max` (int &), `sum` (long &), `cnt` (long &), `totalcnt` (long &): References to variables that store the min, max, sum, count, and total count of values processed.
    Output: Returns the number of buffers read by the thread.
    Data Processing: Reads buffers from `xclient` and processes tuples based on their format. It computes statistics on an integer column, updating the min, max, sum, and counts. The processed buffer is marked as read.
*/
// Calls:
// - env->pts->push() - Pushes profiling timestamps (start and end) to a profiling system
// - xclient.hasNext() - Checks if there are more buffers to read
// - xclient.getBuffer() - Retrieves the next buffer for processing
// - reinterpret_cast<int *>() - Casts the buffer data pointer to an integer pointer
// - xclient.markBufferAsRead() - Marks a buffer as read after processing
// - spdlog::get("XCLIENT")->warn() - Logs warning messages (commented out in the provided code)
// - std::chrono::high_resolution_clock::now() - Retrieves the current time for profiling timestamps




int storageThread(int thr, const std::string &filename);
/* 
Tester::storageThread(int thr, const std::string &filename):
    Purpose: Handles writing the data processed in a thread to a CSV file.
    Input: `thr` (int): The thread number (for logging).
           `filename` (const std::string &): The file name to write the data to.
    Output: Returns the number of buffers read and written to the file.
    Data Processing: Reads buffers from `xclient`, serializes the data according to the schema, and writes the serialized data to a CSV file. Uses different serialization methods based on the data type and schema. Each buffer is processed and written in binary format.
*/
// Function: storageThread
// Calls:
// - env->pts->push - Pushes start profiling timestamp
// - xclient.hasNext - Checks if there is a next buffer to read
// - xclient.getBuffer - Retrieves the next buffer
// - serialize functions - Serializes data to CSV format
// - csvFile.write - Writes serialized data to file
// - xclient.markBufferAsRead - Marks buffer as read
// - spdlog::get("XCLIENT")->info - Logs buffer write status
// - csvFile.close - Closes CSV file

    

};