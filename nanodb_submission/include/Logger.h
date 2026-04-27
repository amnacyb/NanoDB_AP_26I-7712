#pragma once
#include <cstdio>
#include <cstring>
#include <cstdarg>

class Logger {
    FILE*  logFile;
    bool   verbose;   // also print to stdout

    void writeRaw(const char* msg) {
        if (logFile) { fputs(msg, logFile); fflush(logFile); }
        if (verbose)   fputs(msg, stdout);
    }
public:
    explicit Logger(const char* logPath = "nanodb_execution.log", bool verbose = true)
        : logFile(nullptr), verbose(verbose)
    {
        logFile = fopen(logPath, "w");
        if (!logFile) fprintf(stderr, "[WARN] Cannot open log file: %s\n", logPath);
    }
    ~Logger() { if (logFile) fclose(logFile); }

    void log(const char* fmt, ...) {
        char buf[4096];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);

        char out[4200];
        snprintf(out, sizeof(out), "[LOG] %s\n", buf);
        writeRaw(out);
    }

    void info(const char* fmt, ...) {
        char buf[4096];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);

        char out[4200];
        snprintf(out, sizeof(out), "[INFO] %s\n", buf);
        writeRaw(out);
    }

    void warn(const char* fmt, ...) {
        char buf[4096];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);

        char out[4200];
        snprintf(out, sizeof(out), "[WARN] %s\n", buf);
        writeRaw(out);
    }

    void separator(const char* title = nullptr) {
        char out[256];
        if (title)
            snprintf(out, sizeof(out),
                     "\n============================================================\n"
                     "  %s\n"
                     "============================================================\n", title);
        else
            snprintf(out, sizeof(out),
                     "------------------------------------------------------------\n");
        writeRaw(out);
    }

    void raw(const char* fmt, ...) {
        char buf[4096];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);
        writeRaw(buf);
    }

    void setVerbose(bool v) { verbose = v; }
};
