/* Copyright 2022 Bogdan Pilyugin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *   File name: MQTT.c
 *     Project: ChargePointMainboard
 *  Created on: 2022-07-21
 *      Author: Bogdan Pilyugin
 * Description:	
 */
#include <SysConfiguration.h>
#include <SystemApplication.h>
#include "esp_log.h"
#include "Helpers.h"
#include "NetTransport.h"
#include "MQTT.h"
#include "UserCallbacks.h"

#define TAG "MQTT"
#define SERVICE_NAME "SYSTEM"          // Dedicated service name

#define MQTT_DEBUG_MODE  CONFIG_WEBGUIAPP_MQTT_DEBUG_LEVEL

#define MQTT_MESSAGE_BUFER_LENTH 10  //size of mqtt queue
#define MQTT_RECONNECT_CHANGE_ADAPTER  3

#define MQTT_RECONNECT_TIMEOUT 10

#if CONFIG_WEBGUIAPP_MQTT_ENABLE

QueueHandle_t MQTT1MessagesQueueHandle;
QueueHandle_t MQTT2MessagesQueueHandle;

static SemaphoreHandle_t xSemaphoreMQTTHandle = NULL;
static StaticSemaphore_t xSemaphoreMQTTBuf;
static StaticQueue_t xStaticMQTT1MessagesQueue;
static StaticQueue_t xStaticMQTT2MessagesQueue;
uint8_t MQTT1MessagesQueueStorageArea[MQTT_MESSAGE_BUFER_LENTH * sizeof(MQTT_DATA_SEND_STRUCT)];
uint8_t MQTT2MessagesQueueStorageArea[MQTT_MESSAGE_BUFER_LENTH * sizeof(MQTT_DATA_SEND_STRUCT)];

mqtt_client_t mqtt[CONFIG_WEBGUIAPP_MQTT_CLIENTS_NUM] = { 0 };

static void mqtt_system_event_handler(int idx, void *handler_args, esp_event_base_t base, int32_t event_id,
                                      void *event_data);

static void mqtt1_system_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data);
static void mqtt2_system_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data);
static void mqtt1_user_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data);
static void mqtt2_user_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data);

void (*UserEventHandler)(int idx, void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data);
static void *UserArg;
void regUserEventHandler(
        void (*event_handler)(int idx, void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data),
        void *user_arg)
{
    UserEventHandler = event_handler;
    UserArg = user_arg;
}

mqtt_client_t* GetMQTTHandlesPool(int idx)
{
    return &mqtt[idx];
}

QueueHandle_t GetMQTTSendQueue(int idx)
{
    return mqtt[idx].mqtt_queue;
}

bool GetMQTT1Connected(void)
{
    return mqtt[0].is_connected;
}

bool GetMQTT2Connected(void)
{
    return mqtt[1].is_connected;
}

static void log_error_if_nonzero(const char *message, int error_code)
{
    if (error_code != 0)
    {
        ESP_LOGE(TAG, "Last error %s: 0x%x", message, error_code);
    }
}

void ComposeTopic(char *topic, int idx, char *service_name, char *direct)
{
    strcpy((char*) topic, GetSysConf()->mqttStation[idx].SystemName);                 // Global system name
    strcat((char*) topic, "/");
    strcat((char*) topic, GetSysConf()->mqttStation[idx].GroupName);                // Global system name
    strcat((char*) topic, "/");
    strcat((char*) topic, GetSysConf()->mqttStation[idx].ClientID);     // Device client name  (for multiclient devices)
    //strcat((char*) topic, "-");
    //strcat((char*) topic, GetSysConf()->ID);                 //
    strcat((char*) topic, "/");
    strcat((char*) topic, (const char*) service_name);  // Device service name
    strcat((char*) topic, "/");
    strcat((char*) topic, direct);  // Data direction UPLINK or DOWNLINK
}

esp_err_t SysServiceMQTTSend(char *data, int len, int idx)
{
    if (GetMQTTHandlesPool(idx)->mqtt_queue == NULL)
        return ESP_ERR_NOT_FOUND;
    char *buf = (char*) malloc(len);
    if (buf)
    {
        memcpy(buf, data, len);
        MQTT_DATA_SEND_STRUCT DSS = { 0 };
        ComposeTopic(DSS.topic, idx, SERVICE_NAME, UPLINK_SUBTOPIC);
        DSS.raw_data_ptr = buf;
        DSS.data_length = len;
        DSS.keep_memory_onfinish = false;
        if (xQueueSend(GetMQTTHandlesPool(idx)->mqtt_queue, &DSS, pdMS_TO_TICKS(0)) == pdPASS)
            return ESP_OK;
        else
        {
            free(buf);
            ESP_LOGW(TAG, "MQTT message queue is full on client %d", idx);
            return ESP_ERR_TIMEOUT;
        }
    }
    return ESP_ERR_NO_MEM;
}

esp_err_t ExternalServiceMQTTSend(char *servname, char *data, int len, int idx)
{
    if (GetMQTTHandlesPool(idx)->mqtt_queue == NULL)
        return ESP_ERR_NOT_FOUND;
    char *buf = (char*) malloc(len);
    if (buf)
    {
        memcpy(buf, data, len);
        MQTT_DATA_SEND_STRUCT DSS = { 0 };
        ComposeTopic(DSS.topic, idx, servname, UPLINK_SUBTOPIC);
        DSS.raw_data_ptr = buf;
        DSS.data_length = len;
        if (xQueueSend(GetMQTTHandlesPool(idx)->mqtt_queue, &DSS, pdMS_TO_TICKS(0)) == pdPASS)
            return ESP_OK;
        else
        {
            free(buf);
            ESP_LOGW(TAG, "MQTT message queue is full on client %d", idx);
            return ESP_ERR_TIMEOUT;
        }
    }
    return ESP_ERR_NO_MEM;
}

#define MAX_ERROR_JSON  256
/*
 mqtt_app_err_t PublicTestMQTT(int idx)
 {
 char tmp[10];
 char resp[256];
 char JSONMess[512];
 struct jWriteControl jwc;
 jwOpen(&jwc, JSONMess, MAX_ERROR_JSON, JW_OBJECT, JW_PRETTY);
 time_t now;
 time(&now);
 jwObj_int(&jwc, "time", (unsigned int) now);
 jwObj_string(&jwc, "event", "MQTT_TEST_MESSAGE)");
 strcpy(resp, "mqtt://");
 strcat(resp, GetSysConf()->mqttStation[idx].ServerAddr);
 itoa(GetSysConf()->mqttStation[idx].ServerPort, tmp, 10);
 strcat(resp, ":");
 strcat(resp, tmp);
 jwObj_string(&jwc, "url", resp);
 ComposeTopic(resp, idx, SERVICE_NAME, UPLINK_SUBTOPIC);
 jwObj_string(&jwc, "tx_topic", resp);
 ComposeTopic(resp, idx, SERVICE_NAME, DOWNLINK_SUBTOPIC);
 jwObj_string(&jwc, "rx_topic", resp);
 jwEnd(&jwc);
 jwClose(&jwc);
 mqtt_app_err_t merr = API_OK;
 if (SysServiceMQTTSend(JSONMess, strlen(JSONMess), idx) != ESP_OK)
 merr = API_INTERNAL_ERR;
 return merr;
 }
 */

mqtt_app_err_t PublicTestMQTT(int idx)
{
    char tmp[10];
    char resp[256];
    char* JSONMess = malloc(1024);
    struct jWriteControl jwc;
    jwOpen(&jwc, JSONMess, 1024 - 64, JW_OBJECT, JW_COMPACT);
    jwObj_object(&jwc, "data");
    time_t now;
    time(&now);
    jwObj_int(&jwc, "msgid", (unsigned int) now);
    jwObj_string(&jwc, "srcid", GetSysConf()->ID);
    jwObj_string(&jwc, "dstid", "FFFFFFFF");
    char time[ISO8601_TIMESTAMP_LENGTH];
    GetISO8601Time(time);
    jwObj_string(&jwc, "time", time);
    jwObj_int(&jwc, "msgtype", DATA_MESSAGE_TYPE_COMMAND);
    jwObj_int(&jwc, "payloadtype", 1000);
    jwObj_object(&jwc, "payload");
    jwObj_int(&jwc, "applytype", 0);
    jwObj_object(&jwc, "variables");

    rest_var_types tp = VAR_STRING;
    GetConfVar("dev_id", resp, &tp);
    jwObj_string(&jwc, "dev_id", resp);
    GetConfVar("ser_num", resp, &tp);
    jwObj_string(&jwc, "ser_num", resp);
    GetConfVar("model_name", resp, &tp);
    jwObj_string(&jwc, "model_name", resp);
    GetConfVar("hw_rev", resp, &tp);
    jwObj_string(&jwc, "hw_rev", resp);

    tp = VAR_FUNCT;
    GetConfVar("time", resp, &tp);
    jwObj_raw(&jwc, "time", resp);
    GetConfVar("lat", resp, &tp);
    jwObj_raw(&jwc, "lat", resp);
    GetConfVar("lon", resp, &tp);
    jwObj_raw(&jwc, "lon", resp);
    GetConfVar("uptime", resp, &tp);
    jwObj_raw(&jwc, "uptime", resp);
    GetConfVar("free_ram", resp, &tp);
    jwObj_raw(&jwc, "free_ram", resp);
    GetConfVar("free_ram_min", resp, &tp);
    jwObj_raw(&jwc, "free_ram_min", resp);
    GetConfVar("def_interface", resp, &tp);
    jwObj_raw(&jwc, "def_interface", resp);

    GetConfVar("fw_rev", resp, &tp);
    jwObj_raw(&jwc, "fw_rev", resp);
    GetConfVar("idf_rev", resp, &tp);
    jwObj_raw(&jwc, "idf_rev", resp);
#if CONFIG_WEBGUIAPP_WIFI_ENABLE
    GetConfVar("wifi_stat", resp, &tp);
    jwObj_raw(&jwc, "wifi_stat", resp);
    GetConfVar("wifi_level", resp, &tp);
    jwObj_raw(&jwc, "wifi_level", resp);
    tp = VAR_INT;
    GetConfVar("wifi_mode", resp, &tp);
    jwObj_raw(&jwc, "wifi_mode", resp);
    tp = VAR_STRING;
    GetConfVar("wifi_ap_ssid", resp, &tp);
    jwObj_string(&jwc, "wifi_ap_ssid", resp);
    GetConfVar("wifi_sta_ssid", resp, &tp);
    jwObj_string(&jwc, "wifi_sta_ssid", resp);
#endif
#if CONFIG_WEBGUIAPP_GPRS_ENABLE

#endif

    strcpy(resp, "mqtt://");
    strcat(resp, GetSysConf()->mqttStation[idx].ServerAddr);
    itoa(GetSysConf()->mqttStation[idx].ServerPort, tmp, 10);
    strcat(resp, ":");
    strcat(resp, tmp);
    jwObj_string(&jwc, "url", resp);
    ComposeTopic(resp, idx, SERVICE_NAME, UPLINK_SUBTOPIC);
    jwObj_string(&jwc, "tx_topic", resp);
    ComposeTopic(resp, idx, SERVICE_NAME, DOWNLINK_SUBTOPIC);
    jwObj_string(&jwc, "rx_topic", resp);

    jwEnd(&jwc); //close variables
    jwEnd(&jwc); //close payload
    jwEnd(&jwc); //close data
    //calculate sha from 'data' object
    char *datap = strstr(JSONMess, "\"data\":");
    if (datap)
    {
        datap += sizeof("\"data\":") - 1;
        unsigned char sha[32 + 1];
        unsigned char sha_print[32 * 2 + 1];
        SHA256hmacHash((unsigned char*) datap, strlen(datap), (unsigned char*) "mykey", sizeof("mykey"), sha);
        BytesToStr(sha, sha_print, 32);
        sha_print[32 * 2] = 0x00;
#if REAST_API_DEBUG_MODE
            ESP_LOGI(TAG, "SHA256 of DATA object is %s", sha_print);
    #endif
        jwObj_string(&jwc, "signature", (char*) sha_print);
    }
    else
        {
        free(JSONMess);
        return ESP_ERR_NOT_FOUND;
        }

    jwClose(&jwc);
    mqtt_app_err_t merr = API_OK;
    if (SysServiceMQTTSend(JSONMess, strlen(JSONMess), idx) != ESP_OK)
        merr = API_INTERNAL_ERR;
    free(JSONMess);
    return merr;
}

static void mqtt_system_event_handler(int idx, void *handler_args, esp_event_base_t base, int32_t event_id,
                                      void *event_data)
{
    xSemaphoreTake(xSemaphoreMQTTHandle, pdMS_TO_TICKS(1000));
#if MQTT_DEBUG_MODE > 1
    ESP_LOGI(TAG, "Event dispatched from event loop base=%s, event_id=%d", base, (int )event_id);
#endif
    esp_mqtt_event_handle_t event = event_data;
    esp_mqtt_client_handle_t client = event->client;

    int msg_id;
    static int MQTTReconnectCounter = 0; //Change network adapter every MQTT_RECONNECT_CHANGE_ADAPTER number attempts
    char topic[CONFIG_WEBGUIAPP_MQTT_MAX_TOPIC_LENGTH]; //TODO need define max topic length
    switch ((esp_mqtt_event_id_t) event_id)
    {
        case MQTT_EVENT_CONNECTED:
            mqtt[idx].is_connected = true;
            MQTTReconnectCounter = 0;
#if MQTT_DEBUG_MODE > 0
            ESP_LOGI(TAG, "MQTT_EVENT_CONNECTED client %d", idx);
#endif
            ComposeTopic(topic, idx, SERVICE_NAME, DOWNLINK_SUBTOPIC);
msg_id = esp_mqtt_client_subscribe(client, (char*) topic, 0);
#if MQTT_DEBUG_MODE > 0
                        ESP_LOGI(TAG, "sent subscribe successful, msg_id=%d", msg_id);
            ESP_LOGI(TAG, "Subscribe to %s", topic);
#endif

#ifdef  CONFIG_WEBGUIAPP_UART_TRANSPORT_ENABLE
            if (GetSysConf()->serialSettings.Flags.IsBridgeEnabled)
            {
                ComposeTopic(topic, idx, EXTERNAL_SERVICE_NAME, DOWNLINK_SUBTOPIC);
                //Subscribe to the service called "APP"
                msg_id = esp_mqtt_client_subscribe(client, (char*) topic, 0);
#if MQTT_DEBUG_MODE > 0
                ESP_LOGI(TAG, "sent subscribe successful, msg_id=%d", msg_id);
                ESP_LOGI(TAG, "Subscribe to %s", topic);
#endif
            }
#endif
        break;
        case MQTT_EVENT_DISCONNECTED:

#if MQTT_DEBUG_MODE > 0
            ESP_LOGI(TAG, "MQTT_EVENT_DISCONNECTED client %d", idx);
#endif
            if (++MQTTReconnectCounter > MQTT_RECONNECT_CHANGE_ADAPTER)
            {
                MQTTReconnectCounter = 0;
                NextDefaultNetIF();
            }
            mqtt[idx].is_connected = false;
        break;
        case MQTT_EVENT_SUBSCRIBED:

#if MQTT_DEBUG_MODE > 0
            ESP_LOGI(TAG, "MQTT_EVENT_SUBSCRIBED, msg_id=%d", event->msg_id);
#endif
        break;
        case MQTT_EVENT_UNSUBSCRIBED:

#if MQTT_DEBUG_MODE > 0
            ESP_LOGI(TAG, "MQTT_EVENT_UNSUBSCRIBED, msg_id=%d", event->msg_id);
#endif
        break;
        case MQTT_EVENT_PUBLISHED:
            #if MQTT_DEBUG_MODE > 1
            ESP_LOGI(TAG, "MQTT_EVENT_PUBLISHED, msg_id=%d", event->msg_id);
#endif
        break;
        case MQTT_EVENT_DATA:

#if MQTT_DEBUG_MODE > 1
            ESP_LOGI(TAG, "MQTT_EVENT_DATA, client:%d, data-length:%d, offset:%d ", idx, event->data_len, event->current_data_offset);

            ESP_LOGI(TAG, "MQTT client %d data received:%.*s", idx, event->data_len, event->data);
#endif
            if (event->data_len == 0 || event->current_data_offset > 0) //possible fragments of long data
                goto end_of_system_handler;
            //Check if topic is SYSTEM and pass data to handler
            ComposeTopic(topic, idx, SERVICE_NAME, DOWNLINK_SUBTOPIC);
            if (!memcmp(topic, event->topic, event->topic_len))
            {
                //SystemDataHandler(event->data, event->data_len, idx);  //Old API
                char *respbuf = malloc(EXPECTED_MAX_DATA_SIZE);
                if (respbuf != NULL)
                {
                    data_message_t M = { 0 };
                    M.inputDataBuffer = event->data;
                    M.inputDataLength = event->data_len;
                    M.chlidx = idx;
                    M.outputDataBuffer = respbuf;
                    M.outputDataLength = EXPECTED_MAX_DATA_SIZE;
                    ServiceDataHandler(&M);
                    SysServiceMQTTSend(M.outputDataBuffer, strlen(M.outputDataBuffer), idx);
                    free(respbuf);
#if(MQTT_DEBUG_MODE > 1)
                    ESP_LOGI(TAG, "SERVICE data handler on client %d", idx);
#endif
                }
                else
                    ESP_LOGE(TAG, "Out of free RAM for MQTT API handle");
            }

#ifdef  CONFIG_WEBGUIAPP_UART_TRANSPORT_ENABLE
            if (GetSysConf()->serialSettings.Flags.IsBridgeEnabled)
            {
                ComposeTopic(topic, idx, EXTERNAL_SERVICE_NAME, DOWNLINK_SUBTOPIC);
                if (!memcmp(topic, event->topic, event->topic_len))
                {
                    TransmitSerialPort(event->data, event->data_len);
                }
            }
#endif
        break;
        case MQTT_EVENT_ERROR:
            ESP_LOGE(TAG, "MQTT_EVENT_ERROR, client %d", idx);
            if (event->error_handle->error_type == MQTT_ERROR_TYPE_TCP_TRANSPORT)
            {
                log_error_if_nonzero("reported from esp-tls", event->error_handle->esp_tls_last_esp_err);
                log_error_if_nonzero("reported from tls stack", event->error_handle->esp_tls_stack_err);
                log_error_if_nonzero("captured as transport's socket errno",
                                     event->error_handle->esp_transport_sock_errno);
                ESP_LOGI(TAG, "Last errno string (%s)", strerror(event->error_handle->esp_transport_sock_errno));
            }
        break;
        default:
            #if MQTT_DEBUG_MODE > 0
            ESP_LOGI(TAG, "Other event id:%d", event->event_id);
#endif
        break;
    }
end_of_system_handler:
    xSemaphoreGive(xSemaphoreMQTTHandle);
}

static void reconnect_MQTT_handler(void *arg, esp_event_base_t event_base,
                                   int32_t event_id,
                                   void *event_data)
{
    if (event_id == IP_EVENT_ETH_GOT_IP ||
            event_id == IP_EVENT_STA_GOT_IP ||
            event_id == IP_EVENT_PPP_GOT_IP)
    {

        for (int i = 0; i < CONFIG_WEBGUIAPP_MQTT_CLIENTS_NUM; ++i)
        {
            if (mqtt[i].mqtt && mqtt[i].is_connected)
            {
                esp_mqtt_client_disconnect(mqtt[i].mqtt);
                esp_mqtt_client_reconnect(mqtt[i].mqtt);
            }
        }
    }
}

void MQTTStart(void)
{
    for (int i = 0; i < CONFIG_WEBGUIAPP_MQTT_CLIENTS_NUM; ++i)
    {
        if (mqtt[i].mqtt)
            esp_mqtt_client_reconnect(mqtt[i].mqtt);
    }
}

void MQTTStop(void)
{
    for (int i = 0; i < CONFIG_WEBGUIAPP_MQTT_CLIENTS_NUM; ++i)
    {
        if (mqtt[i].mqtt)
            esp_mqtt_client_disconnect(mqtt[i].mqtt);
    }
}

void MQTTReconnect(void)
{
    for (int i = 0; i < CONFIG_WEBGUIAPP_MQTT_CLIENTS_NUM; ++i)
    {
        if (mqtt[i].mqtt)
        {
            esp_mqtt_client_disconnect(mqtt[i].mqtt);
            esp_mqtt_client_reconnect(mqtt[i].mqtt);
        }
    }
}

void MQTTTaskTransmit(void *pvParameter)
{
    MQTT_DATA_SEND_STRUCT DSS;
    int idx = *(int*) pvParameter;
    while (!mqtt[idx].mqtt_queue)
        vTaskDelay(pdMS_TO_TICKS(300)); //wait for MQTT queue ready
    while (1)
    {
        while (!mqtt[idx].is_connected)
            vTaskDelay(pdMS_TO_TICKS(300));
        xQueueReceive(mqtt[idx].mqtt_queue, &DSS, portMAX_DELAY);
        if (mqtt[idx].mqtt && mqtt[idx].is_connected)
        {

#if MQTT_DEBUG_MODE > 1
            ESP_LOGW(TAG, "MQTT client %d data send:%.*s", idx, DSS.data_length, DSS.raw_data_ptr);
#endif
            esp_mqtt_client_publish(mqtt[idx].mqtt,
                                    (const char*) DSS.topic,
                                    (const char*) DSS.raw_data_ptr,
                                    DSS.data_length,
                                    0, 0);
        }
        if (!DSS.keep_memory_onfinish)
        {
            free(DSS.raw_data_ptr);
        }
    }
}

static void start_mqtt()
{
    esp_mqtt_client_config_t mqtt_cfg = { 0 };

    char url[CONFIG_WEBGUIAPP_MQTT_MAX_TOPIC_LENGTH + 14];
    char tmp[128];

    for (int i = 0; i < CONFIG_WEBGUIAPP_MQTT_CLIENTS_NUM; ++i)
    {
        if (GetSysConf()->mqttStation[i].Flags1.bIsGlobalEnabled)
        {
            strcpy(url, "mqtt://");
            strcat(url, GetSysConf()->mqttStation[i].ServerAddr);
            itoa(GetSysConf()->mqttStation[i].ServerPort, tmp, 10);
            strcat(url, ":");
            strcat(url, tmp);
#if ESP_IDF_VERSION_MAJOR >= 5
            mqtt_cfg.buffer.out_size = EXPECTED_MAX_DATA_SIZE;
            mqtt_cfg.buffer.size = EXPECTED_MAX_DATA_SIZE;
            mqtt_cfg.broker.address.uri = url;
            mqtt_cfg.credentials.username = GetSysConf()->mqttStation[i].UserName;
            mqtt_cfg.credentials.authentication.password = GetSysConf()->mqttStation[i].UserPass;
#else
            mqtt_cfg.uri = url;
            mqtt_cfg.username = GetSysConf()->mqttStation[i].UserName;
            mqtt_cfg.password = GetSysConf()->mqttStation[i].UserPass;
#endif
            strcpy(tmp, GetSysConf()->mqttStation[i].ClientID);
            strcat(tmp, "-");
            strcat(tmp, GetSysConf()->ID);
#if ESP_IDF_VERSION_MAJOR >= 5
            mqtt_cfg.credentials.client_id = tmp;
            mqtt_cfg.network.reconnect_timeout_ms = MQTT_RECONNECT_TIMEOUT * 1000;
#else
            mqtt_cfg.client_id = tmp;
            mqtt_cfg.reconnect_timeout_ms = MQTT_RECONNECT_TIMEOUT * 1000;
#endif
            mqtt[i].is_connected = false;
            mqtt[i].mqtt_index = i;
            //mqtt_cfg.user_context = (void*) &mqtt[i];
            mqtt[i].mqtt = esp_mqtt_client_init(&mqtt_cfg);
            /* The last argument may be used to pass data to the event handler, in this example mqtt_system_event_handler */
            esp_mqtt_client_register_event(mqtt[i].mqtt, ESP_EVENT_ANY_ID, mqtt[i].system_event_handler, &mqtt[i]);
            esp_mqtt_client_register_event(mqtt[i].mqtt, ESP_EVENT_ANY_ID, mqtt[i].user_event_handler, &mqtt[i]);
            esp_event_handler_register(IP_EVENT, ESP_EVENT_ANY_ID, &reconnect_MQTT_handler, &mqtt[i].mqtt);
            esp_mqtt_client_start(mqtt[i].mqtt);
            xTaskCreate(MQTTTaskTransmit, "MQTTTaskTransmit", 1024 * 4, (void*) &mqtt[i].mqtt_index, 3, NULL);
        }
    }
}

void MQTTRun(void)
{
    xSemaphoreMQTTHandle = xSemaphoreCreateBinaryStatic(&xSemaphoreMQTTBuf);
    xSemaphoreGive(xSemaphoreMQTTHandle);

    MQTT1MessagesQueueHandle = NULL;
    MQTT2MessagesQueueHandle = NULL;
    if (GetSysConf()->mqttStation[0].Flags1.bIsGlobalEnabled)
        MQTT1MessagesQueueHandle = xQueueCreateStatic(MQTT_MESSAGE_BUFER_LENTH,
                                                      sizeof(MQTT_DATA_SEND_STRUCT),
                                                      MQTT1MessagesQueueStorageArea,
                                                      &xStaticMQTT1MessagesQueue);
    mqtt[0].mqtt_queue = MQTT1MessagesQueueHandle;

#if CONFIG_WEBGUIAPP_MQTT_CLIENTS_NUM == 2
    if (GetSysConf()->mqttStation[1].Flags1.bIsGlobalEnabled)
        MQTT2MessagesQueueHandle = xQueueCreateStatic(MQTT_MESSAGE_BUFER_LENTH,
                                                      sizeof(MQTT_DATA_SEND_STRUCT),
                                                      MQTT2MessagesQueueStorageArea,
                                                      &xStaticMQTT2MessagesQueue);
    mqtt[1].mqtt_queue = MQTT2MessagesQueueHandle;
#endif

    mqtt[0].system_event_handler = mqtt1_system_event_handler;
    mqtt[0].user_event_handler = mqtt1_user_event_handler;
    mqtt[0].user_arg = UserArg;
    mqtt[1].system_event_handler = mqtt2_system_event_handler;
    mqtt[1].user_event_handler = mqtt2_user_event_handler;
    mqtt[1].user_arg = UserArg;
    start_mqtt();
}

static void mqtt1_system_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    mqtt_system_event_handler(0, handler_args, base, event_id, event_data);
}
static void mqtt2_system_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    mqtt_system_event_handler(1, handler_args, base, event_id, event_data);
}
static void mqtt1_user_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    UserEventHandler(0, handler_args, base, event_id, event_data);
}
static void mqtt2_user_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    UserEventHandler(1, handler_args, base, event_id, event_data);
}

#endif

#define MAX_MQTT_LOG_MESSAGE (1024)
#define SPIRAL_LOG_TAG "SystemExtendedLog"
//char data[MAX_MQTT_LOG_MESSAGE];
esp_err_t ExtendedLog(esp_log_level_t level, char *format, ...)
{
    va_list arg;
    va_start(arg, format);
    va_end(arg);
    char *data = (char*) malloc(MAX_MQTT_LOG_MESSAGE);
    if (data == NULL)
        return ESP_ERR_NO_MEM;
    vsnprintf(data, MAX_MQTT_LOG_MESSAGE, format, arg);
    if (strlen(data) == MAX_MQTT_LOG_MESSAGE - 1)
        for (int i = 0; i < 3; i++)
            *(data + MAX_MQTT_LOG_MESSAGE - 2 - i) = '.';
    switch (level)
    {
        case ESP_LOG_INFO:
            case ESP_LOG_DEBUG:
            case ESP_LOG_VERBOSE:
            case ESP_LOG_NONE:
            ESP_LOGI(SPIRAL_LOG_TAG, "%s", data);
        break;
        case ESP_LOG_WARN:
            ESP_LOGW(SPIRAL_LOG_TAG, "%s", data);
        break;
        case ESP_LOG_ERROR:
            ESP_LOGE(SPIRAL_LOG_TAG, "%s", data);
        break;
    }

    for (int idx = 0; idx < 2; idx++)
    {
        if (GetMQTTHandlesPool(idx)->mqtt_queue == NULL)
            continue;
        char time[RFC3339_TIMESTAMP_LENGTH];
        GetRFC3339Time(time);
        char *buf = (char*) malloc(strlen(data) + RFC3339_TIMESTAMP_LENGTH + 2);
        if (buf)
        {
            strcpy(buf, time);
            strcat(buf, "  ");
            strcat(buf, data);
            MQTT_DATA_SEND_STRUCT DSS = { 0 };
            ComposeTopic(DSS.topic, idx, "LOG", "UPLINK");
            DSS.raw_data_ptr = buf;
            DSS.data_length = strlen(buf);
            DSS.keep_memory_onfinish = false;
            if (xQueueSend(GetMQTTHandlesPool(idx)->mqtt_queue, &DSS, pdMS_TO_TICKS(0)) != pdPASS)
                free(buf);
            continue;
        }
        free(data);
        return ESP_ERR_NO_MEM;
    }
    free(data);
    return ESP_OK;
}

