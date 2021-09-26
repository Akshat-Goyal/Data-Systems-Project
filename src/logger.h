#include <iostream>
#include <bits/stdc++.h>
#include <sys/stat.h>
#include <fstream>

using namespace std;

typedef unsigned int uint;

class Logger
{
    // Turn it on if you want to check the log. It is off by default
    bool TO_LOG = false;

    string logFile = "log";
    ofstream fout;

public:
    Logger();
    void log(string logString);
};

extern Logger logger;
