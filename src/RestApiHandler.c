/*! Copyright 2023 Bogdan Pilyugin
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
 *  	 \file RestApiHandler.c
 *    \version 1.0
 * 		 \date 2023-07-26
 *     \author Bogdan Pilyugin
 * 	    \brief    
 *    \details 
 *	\copyright Apache License, Version 2.0
 */

#include "SystemApplication.h"
#include <SysConfiguration.h>
#include <webguiapp.h>
#include "esp_wifi.h"
#include "esp_netif.h"
#include "esp_ota_ops.h"
#include "romfs.h"
#include "esp_idf_version.h"
#include "NetTransport.h"

extern SYS_CONFIG SysConfig;


rest_var_t *AppVars = NULL;
int AppVarsSize = 0;
void SetAppVars( rest_var_t* appvars, int size)
{
    AppVars = appvars;
    AppVarsSize = size;
}

static void PrintInterfaceState(char *argres, int rw, esp_netif_t *netif)
{
    snprintf(argres, MAX_DYNVAR_LENGTH,
             (netif != NULL && esp_netif_is_netif_up(netif)) ? "\"CONNECTED\"" : "\"DISCONNECTED\"");
}

static void funct_wifi_stat(char *argres, int rw)
{
    if (GetSysConf()->wifiSettings.WiFiMode == WIFI_MODE_AP)
        PrintInterfaceState(argres, rw, GetAPNetifAdapter());
    else
        PrintInterfaceState(argres, rw, GetSTANetifAdapter());
}

#if CONFIG_WEBGUIAPP_ETHERNET_ENABLE
static void funct_eth_stat(char *argres, int rw)
{
    PrintInterfaceState(argres, rw, GetETHNetifAdapter());
}
#endif

#if CONFIG_WEBGUIAPP_GPRS_ENABLE
static void funct_gsm_stat(char *argres, int rw)
{
    PrintInterfaceState(argres, rw, GetPPPNetifAdapter());
}
#endif

static void funct_mqtt_1_stat(char *argres, int rw)
{
    snprintf(argres, MAX_DYNVAR_LENGTH, (GetMQTT1Connected()) ? "\"CONNECTED\"" : "\"DISCONNECTED\"");
}
static void funct_mqtt_2_stat(char *argres, int rw)
{
    snprintf(argres, MAX_DYNVAR_LENGTH, (GetMQTT2Connected()) ? "\"CONNECTED\"" : "\"DISCONNECTED\"");
}
static void funct_def_interface(char *argres, int rw)
{
    GetDefaultNetIFName(argres);
}

static void funct_time(char *argres, int rw)
{
    time_t now;
    time(&now);
    snprintf(argres, MAX_DYNVAR_LENGTH, "%d", (int) now);
}

static void funct_uptime(char *argres, int rw)
{
    snprintf(argres, MAX_DYNVAR_LENGTH, "%d", (int) GetUpTime());
}
static void funct_wifi_level(char *argres, int rw)
{
    wifi_ap_record_t wifi;
    if (esp_wifi_sta_get_ap_info(&wifi) == ESP_OK)
        snprintf(argres, MAX_DYNVAR_LENGTH, "\"%ddBm\"", wifi.rssi);
    else
        snprintf(argres, MAX_DYNVAR_LENGTH, "\"-\"");
}

static void funct_fram(char *argres, int rw)
{
    snprintf(argres, MAX_DYNVAR_LENGTH, "%d", (int) esp_get_free_heap_size());
}
static void funct_fram_min(char *argres, int rw)
{
    snprintf(argres, MAX_DYNVAR_LENGTH, "%d", (int) esp_get_minimum_free_heap_size());
}

static void funct_idf_ver(char *argres, int rw)
{
    esp_app_desc_t cur_app_info;
    if (esp_ota_get_partition_description(esp_ota_get_running_partition(), &cur_app_info) == ESP_OK)
        snprintf(argres, MAX_DYNVAR_LENGTH, "\"%s\"", cur_app_info.idf_ver);
    else
        snprintf(argres, MAX_DYNVAR_LENGTH, "%s", "ESP_ERR_NOT_SUPPORTED");
}

static void funct_fw_ver(char *argres, int rw)
{
    esp_app_desc_t cur_app_info;
    if (esp_ota_get_partition_description(esp_ota_get_running_partition(), &cur_app_info) == ESP_OK)
        snprintf(argres, MAX_DYNVAR_LENGTH, "\"%s\"", cur_app_info.version);
    else
        snprintf(argres, MAX_DYNVAR_LENGTH, "%s", "ESP_ERR_NOT_SUPPORTED");
}

static void funct_build_date(char *argres, int rw)
{
    esp_app_desc_t cur_app_info;
    if (esp_ota_get_partition_description(esp_ota_get_running_partition(), &cur_app_info) == ESP_OK)
        snprintf(argres, MAX_DYNVAR_LENGTH, "\"%s %s\"", cur_app_info.date, cur_app_info.time);
    else
        snprintf(argres, MAX_DYNVAR_LENGTH, "%s", "ESP_ERR_NOT_SUPPORTED");
}

static void funct_wifiscan(char *argres, int rw)
{
    if (atoi(argres))
        WiFiScan();
}

static void funct_wifiscanres(char *argres, int rw)
{
    int arg = atoi(argres);
    wifi_ap_record_t *Rec;
    struct jWriteControl jwc;
    jwOpen(&jwc, argres, VAR_MAX_VALUE_LENGTH, JW_ARRAY, JW_COMPACT);
    for (int i = 0; i < arg; i++)
    {
        Rec = GetWiFiAPRecord(i);
        if (Rec)
        {
            jwArr_object(&jwc);
            jwObj_string(&jwc, "ssid", (char*) Rec->ssid);
            jwObj_int(&jwc, "rssi", Rec->rssi);
            jwObj_int(&jwc, "ch", Rec->primary);
            jwEnd(&jwc);
        }
    }
    int err = jwClose(&jwc);
    if (err == JWRITE_OK)
        return;
    if(err > JWRITE_BUF_FULL )
        strcpy(argres, "\"SYS_ERROR_NO_MEMORY\"");
    else
        strcpy(argres, "\"SYS_ERROR_UNKNOWN\"");
}

const int hw_rev = CONFIG_BOARD_HARDWARE_REVISION;



const rest_var_t SystemVariables[] =
        {
                /*FUNCTIONS*/
                { 0, "time", &funct_time, VAR_FUNCT, R, 0, 0 },
                { 0, "uptime", &funct_uptime, VAR_FUNCT, R, 0, 0 },

                { 0, "free_ram", &funct_fram, VAR_FUNCT, R, 0, 0 },
                { 0, "free_ram_min", &funct_fram_min, VAR_FUNCT, R, 0, 0 },
                { 0, "def_interface", &funct_def_interface, VAR_FUNCT, R, 0, 0 },

                { 0, "fw_rev", &funct_fw_ver, VAR_FUNCT, R, 0, 0 },
                { 0, "idf_rev", &funct_idf_ver, VAR_FUNCT, R, 0, 0 },
                { 0, "build_date", &funct_build_date, VAR_FUNCT, R, 0, 0 },

                /*CONSTANTS*/
                { 0, "model_name", CONFIG_DEVICE_MODEL_NAME, VAR_STRING, R, 1, 64 },
                { 0, "hw_rev", ((int*) &hw_rev), VAR_INT, R, 1, 1024 },
                { 0, "build_date", CONFIG_DEVICE_MODEL_NAME, VAR_STRING, R, 1, 64 },
                { 0, "model_name", CONFIG_DEVICE_MODEL_NAME, VAR_STRING, R, 1, 64 },

                /*VARIABLES*/
                { 0, "net_bios_name", &SysConfig.NetBIOSName, VAR_STRING, RW, 3, 31 },
                { 0, "sys_name", &SysConfig.SysName, VAR_STRING, RW, 3, 31 },
                { 0, "sys_pass", &SysConfig.SysPass, VAR_PASS, RW, 3, 31 },
                { 0, "ota_url", &SysConfig.OTAURL, VAR_STRING, RW, 3, 128 },
                { 0, "ota_auto_int", &SysConfig.OTAAutoInt, VAR_INT, RW, 0, 65535 },
                { 0, "ser_num", &SysConfig.SN, VAR_STRING, RW, 10, 10 },
                { 0, "dev_id", &SysConfig.ID, VAR_STRING, RW, 8, 8 },
                { 0, "color_scheme", &SysConfig.ColorSheme, VAR_INT, RW, 1, 2 },

                { 0, "ota_enab", &SysConfig.Flags1.bIsOTAEnabled, VAR_BOOL, RW, 0, 1 },
                { 0, "res_ota_enab", &SysConfig.Flags1.bIsResetOTAEnabled, VAR_BOOL, RW, 0, 1 },
                { 0, "led_enab", &SysConfig.Flags1.bIsLedsEnabled, VAR_BOOL, RW, 0, 1 },
                { 0, "lora_confirm", &SysConfig.Flags1.bIsLoRaConfirm, VAR_BOOL, RW, 0, 1 },
                { 0, "tcp_confirm", &SysConfig.Flags1.bIsTCPConfirm, VAR_BOOL, RW, 0, 1 },

                { 0, "sntp_timezone", &SysConfig.sntpClient.TimeZone, VAR_INT, RW, 0, 23 },
                { 0, "sntp_serv1", &SysConfig.sntpClient.SntpServerAdr, VAR_STRING, RW, 3, 32 },
                { 0, "sntp_serv2", &SysConfig.sntpClient.SntpServer2Adr, VAR_STRING, RW, 3, 32 },
                { 0, "sntp_serv3", &SysConfig.sntpClient.SntpServer3Adr, VAR_STRING, RW, 3, 32 },
                { 0, "sntp_enab", &SysConfig.sntpClient.Flags1.bIsGlobalEnabled, VAR_BOOL, RW, 0, 1 },

#if CONFIG_WEBGUIAPP_MQTT_ENABLE
                { 0, "mqtt_1_enab", &SysConfig.mqttStation[0].Flags1.bIsGlobalEnabled, VAR_BOOL, RW, 0, 1 },
                { 0, "mqtt_1_serv", &SysConfig.mqttStation[0].ServerAddr, VAR_STRING, RW, 3, 63 },
                { 0, "mqtt_1_port", &SysConfig.mqttStation[0].ServerPort, VAR_INT, RW, 1, 65534 },
                { 0, "mqtt_1_syst", &SysConfig.mqttStation[0].SystemName, VAR_STRING, RW, 3, 31 },
                { 0, "mqtt_1_group", &SysConfig.mqttStation[0].GroupName, VAR_STRING, RW, 3, 31 },
                { 0, "mqtt_1_clid", &SysConfig.mqttStation[0].ClientID, VAR_STRING, RW, 3, 31 },
                { 0, "mqtt_1_uname", &SysConfig.mqttStation[0].UserName, VAR_STRING, RW, 3, 31 },
                { 0, "mqtt_1_pass", &SysConfig.mqttStation[0].UserPass, VAR_PASS, RW, 3, 31 },
                { 0, "mqtt_1_stat", &funct_mqtt_1_stat, VAR_FUNCT, R, 0, 0 },

#if CONFIG_WEBGUIAPP_MQTT_CLIENTS_NUM == 2
                { 0, "mqtt_2_enab", &SysConfig.mqttStation[1].Flags1.bIsGlobalEnabled, VAR_BOOL, RW, 0, 1 },
                { 0, "mqtt_2_serv", &SysConfig.mqttStation[1].ServerAddr, VAR_STRING, RW, 3, 63 },
                { 0, "mqtt_2_port", &SysConfig.mqttStation[1].ServerPort, VAR_INT, RW, 1, 65534 },
                { 0, "mqtt_2_syst", &SysConfig.mqttStation[1].SystemName, VAR_STRING, RW, 3, 31 },
                { 0, "mqtt_2_group", &SysConfig.mqttStation[1].GroupName, VAR_STRING, RW, 3, 31 },
                { 0, "mqtt_2_clid", &SysConfig.mqttStation[1].ClientID, VAR_STRING, RW, 3, 31 },
                { 0, "mqtt_2_uname", &SysConfig.mqttStation[1].UserName, VAR_STRING, RW, 3, 31 },
                { 0, "mqtt_2_pass", &SysConfig.mqttStation[1].UserPass, VAR_PASS, RW, 3, 31 },
                { 0, "mqtt_2_stat", &funct_mqtt_2_stat, VAR_FUNCT, R, 0, 0 },

#endif
#endif

#if CONFIG_WEBGUIAPP_ETHERNET_ENABLE
                { 0, "eth_ip", &SysConfig.wifiSettings.InfIPAddr, VAR_IPADDR,RW, 0, 0 },
                { 0, "eth_mask", &SysConfig.wifiSettings.InfMask, VAR_IPADDR,RW, 0, 0 },
                { 0, "eth_gw", &SysConfig.wifiSettings.InfGateway, VAR_IPADDR,RW, 0, 0 },
                { 0, "eth_dns1", &SysConfig.wifiSettings.DNSAddr1, VAR_IPADDR,RW, 0, 0 },
                { 0, "eth_dns2", &SysConfig.wifiSettings.DNSAddr2, VAR_IPADDR,RW, 0, 0 },
                { 0, "eth_dns3", &SysConfig.wifiSettings.DNSAddr3, VAR_IPADDR,RW, 0, 0 },
                { 0, "eth_stat", &funct_eth_stat, VAR_FUNCT, R, 0, 0 },



#endif

#if CONFIG_WEBGUIAPP_WIFI_ENABLE
                { 0, "wifi_mode", &SysConfig.wifiSettings.WiFiMode, VAR_INT, RW, 1, 3 },
                { 0, "wifi_sta_ip", &SysConfig.wifiSettings.InfIPAddr, VAR_IPADDR, RW, 0, 0 },
                { 0, "wifi_sta_mask", &SysConfig.wifiSettings.InfMask, VAR_IPADDR, RW, 0, 0 },
                { 0, "wifi_sta_gw", &SysConfig.wifiSettings.InfGateway, VAR_IPADDR, RW, 0, 0 },
                { 0, "wifi_ap_ip", &SysConfig.wifiSettings.ApIPAddr, VAR_IPADDR, RW, 0, 0 },
                { 0, "wifi_dns1", &SysConfig.wifiSettings.DNSAddr1, VAR_IPADDR, RW, 0, 0 },
                { 0, "wifi_dns2", &SysConfig.wifiSettings.DNSAddr2, VAR_IPADDR, RW, 0, 0 },
                { 0, "wifi_dns3", &SysConfig.wifiSettings.DNSAddr3, VAR_IPADDR, RW, 0, 0 },
                { 0, "wifi_sta_ssid", &SysConfig.wifiSettings.InfSSID, VAR_STRING, RW, 3, 31 },
                { 0, "wifi_sta_key", &SysConfig.wifiSettings.InfSecurityKey, VAR_PASS, RW, 8, 31 },
                { 0, "wifi_ap_ssid", &SysConfig.wifiSettings.ApSSID, VAR_STRING, RW, 3, 31 },
                { 0, "wifi_ap_key", &SysConfig.wifiSettings.ApSecurityKey, VAR_PASS, RW, 8, 31 },

                { 0, "wifi_enab", &SysConfig.wifiSettings.Flags1.bIsWiFiEnabled, VAR_BOOL, RW, 0, 1 },
                { 0, "wifi_isdhcp", &SysConfig.wifiSettings.Flags1.bIsDHCPEnabled, VAR_BOOL, RW, 0, 1 },
                { 0, "wifi_power", &SysConfig.wifiSettings.MaxPower, VAR_INT, RW, 0, 80 },
                { 0, "wifi_stat", &funct_wifi_stat, VAR_FUNCT, R, 0, 0 },
                { 0, "wifi_scan", &funct_wifiscan, VAR_FUNCT, R, 0, 0 },
                { 0, "wifi_scan_res", &funct_wifiscanres, VAR_FUNCT, R, 0, 0 },
                { 0, "wifi_level", &funct_wifi_level, VAR_FUNCT, R, 0, 0 },
        #endif

#if CONFIG_WEBGUIAPP_GPRS_ENABLE
                { 0, "gsm_stat", &funct_gsm_stat, VAR_FUNCT, R, 0, 0 },

#endif

        };

esp_err_t SetConfVar(char *name, char *val, rest_var_types *tp)
{
    rest_var_t *V = NULL;
    //Search for system variables
    for (int i = 0; i < sizeof(SystemVariables) / sizeof(rest_var_t); ++i)
    {
        if (!strcmp(SystemVariables[i].alias, name))
        {
            V = (rest_var_t*) (&SystemVariables[i]);
            break;
        }
    }
    //Search for user variables
    if(AppVars)
    {
        for (int i = 0; i < AppVarsSize; ++i)
        {
            if (!strcmp(AppVars[i].alias, name))
            {
                V = (rest_var_t*) (&AppVars[i]);
                break;
            }
        }
    }

    if (!V)
        return ESP_ERR_NOT_FOUND;
    if (V->varattr == R)
        return ESP_OK;
    int constr;
    *tp = V->vartype;
    switch (V->vartype)
    {
        case VAR_BOOL:
            if (!strcmp(val, "true") || !strcmp(val, "1"))
                *((bool*) V->ref) = true;
            else if (!strcmp(val, "false") || !strcmp(val, "0"))
                *((bool*) V->ref) = 0;
            else
                return ESP_ERR_INVALID_ARG;
        break;
        case VAR_INT:
            constr = atoi(val);
            if (constr < V->minlen || constr > V->maxlen)
                return ESP_ERR_INVALID_ARG;
            *((int*) V->ref) = constr;
        break;
        case VAR_STRING:
            constr = strlen(val);
            if (constr < V->minlen || constr > V->maxlen)
                return ESP_ERR_INVALID_ARG;
            strcpy(V->ref, val);
        break;
        case VAR_PASS:
            if (val[0] != '*')
            {
                constr = strlen(val);
                if (constr < V->minlen || constr > V->maxlen)
                    return ESP_ERR_INVALID_ARG;
                strcpy(V->ref, val);
            }
        break;

        case VAR_IPADDR:
            esp_netif_str_to_ip4(val, (esp_ip4_addr_t*) (V->ref));
        break;
        case VAR_FUNCT:
            ((void (*)(char*, int)) (V->ref))(val, 1);
        break;
        case VAR_ERROR:
            break;

    }
    return ESP_OK;
}

esp_err_t GetConfVar(char *name, char *val, rest_var_types *tp)
{
    rest_var_t *V = NULL;
    for (int i = 0; i < sizeof(SystemVariables) / sizeof(rest_var_t); ++i)
    {
        if (!strcmp(SystemVariables[i].alias, name))
        {
            V = (rest_var_t*) (&SystemVariables[i]);
            break;
        }
    }
    //Search for user variables
    if(AppVars)
    {
        for (int i = 0; i < AppVarsSize; ++i)
        {
            if (!strcmp(AppVars[i].alias, name))
            {
                V = (rest_var_t*) (&AppVars[i]);
                break;
            }
        }
    }
    if (!V)
        return ESP_ERR_NOT_FOUND;
    *tp = V->vartype;
    switch (V->vartype)
    {
        case VAR_BOOL:
            strcpy(val, *((bool*) V->ref) ? "true" : "false");
        break;
        case VAR_INT:
            itoa(*((int*) V->ref), val, 10);
        break;
        case VAR_STRING:
            strcpy(val, (char*) V->ref);
        break;
        case VAR_PASS:
            strcpy(val, "******");
        break;
        case VAR_IPADDR:
            esp_ip4addr_ntoa((const esp_ip4_addr_t*) V->ref, val, 16);
        break;
        case VAR_FUNCT:
            ((void (*)(char*, int)) (V->ref))(val, 1);
        break;
        case VAR_ERROR:
            break;
    }

    //val = V->ref;
    return ESP_OK;
}

