#include <iostream>
#include <bits/stdc++.h>
#include <sys/stat.h>
#include <fstream>

using namespace std;

typedef unsigned int uint;

class Logger
{
    string logFile = "log";
    ofstream fout;

public:
    Logger();
    void log(string logString);
};

extern Logger logger;
