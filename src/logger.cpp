#include "global.h"

Logger::Logger()
{
    if (this->TO_LOG)
        this->fout.open(this->logFile, ios::out);
}

void Logger::log(string logString)
{
    if (this->TO_LOG)
        fout << logString << endl;
}
