#pragma once

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_task_wdt.h"

#include <Arduino.h>
#include <Ticker.h>
#include "ArduinoNvs.h"

#include "SystemConfig.h"
#include "WifiConfig.h"
#include "globals.h"
#include "sys_log.h"
#include "notifyClients.h"

#include "task_syscontrol.h"
#include "Config.h"
#include "LogConfig.h"
#include "esp32_repartition.h"
#include "ArduinoNvs.h"


#ifdef ESPRESSIF32_3_5_0
#include <LITTLEFS.h>
#else
#include "LittleFS.h"
#endif

// globals
extern Ticker TaskConfigAndLogTimeout;
extern TaskHandle_t xTaskConfigAndLogHandle;
extern uint32_t TaskConfigAndLogHWmark;
extern volatile bool saveRemotesflag;
extern volatile bool saveVremotesflag;
extern bool chkpartition;
extern int chk_partition_res;
extern bool formatFileSystem;
extern bool flashLogInitReady;

void startTaskConfigAndLog();
void TaskConfigAndLog(void *pvParameters);
void execLogAndConfigTasks();

void syslog_queue_worker();
bool initFileSystem();
void logInit();
void log_mem_info();