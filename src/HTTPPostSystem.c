/*! Copyright 2022 Bogdan Pilyugin
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
 *  	 \file HTTPPostSystem.c
 *    \version 1.0
 * 		 \date 2022-08-14
 *     \author Bogdan Pilyugin
 * 	    \brief    
 *    \details 
 *	\copyright Apache License, Version 2.0
 */

#include "HTTPServer.h"

static const char *TAG = "HTTPServerPost";

#define FILE_PATH_MAX (ESP_VFS_PATH_MAX + CONFIG_SPIFFS_OBJ_NAME_LEN)

const char url_eth_settings[] = "set_eth.html";
const char url_wifi_settings[] = "set_wifi.html";
const char url_gprs_settings[] = "set_gprs.html";
const char url_mqtt_settings[] = "set_mqtt.html";
const char url_sys_settings[] = "set_sys.html";
const char url_time_settings[] = "set_time.html";
const char url_reboot[] = "reboot.html";

static HTTP_IO_RESULT AfterPostHandler(httpd_req_t *req, const char *filename, char *PostData);

static HTTP_IO_RESULT HTTPPostEthernetSettings(httpd_req_t *req, char *PostData);
static HTTP_IO_RESULT HTTPPostWiFiSettings(httpd_req_t *req, char *PostData);
static HTTP_IO_RESULT HTTPPostGPRSSettings(httpd_req_t *req, char *PostData);
static HTTP_IO_RESULT HTTPPostMQTTSettings(httpd_req_t *req, char *PostData);
static HTTP_IO_RESULT HTTPPostSystemSettings(httpd_req_t *req, char *PostData);
static HTTP_IO_RESULT HTTPPostTimeSettings(httpd_req_t *req, char *PostData);
static HTTP_IO_RESULT HTTPPostReboot(httpd_req_t *req, char *PostData);


HTTP_IO_RESULT (*AfterPostHandlerCust)(httpd_req_t *req, const char *filename, char *PostData);

void regAfterPostHandlerCustom(HTTP_IO_RESULT (*post_handler)(httpd_req_t *req, const char *filename, char *PostData))
{
    AfterPostHandlerCust = post_handler;
}

HTTP_IO_RESULT HTTPPostApp(httpd_req_t *req, const char *filename, char *PostData)
{
    const char *pt = filename + 1;

#if HTTP_SERVER_DEBUG_LEVEL > 0
    ESP_LOGI(TAG, "URI for POST processing:%s", req->uri);
    ESP_LOGI(TAG, "Filename:%s", pt);
    ESP_LOGI(TAG, "DATA for POST processing:%s", PostData);
#endif

    HTTP_IO_RESULT res = AfterPostHandler(req, pt, PostData);

    switch (res)
    {
        case HTTP_IO_DONE:
            break;
        case HTTP_IO_WAITING:
            break;
        case HTTP_IO_NEED_DATA:
            break;
        case HTTP_IO_REDIRECT:
            strcpy((char*)filename, PostData);
        break;
    }
    return res;
}

static HTTP_IO_RESULT AfterPostHandler(httpd_req_t *req, const char *filename, char *PostData)
{
    if (!memcmp(filename, url_eth_settings, sizeof(url_eth_settings)))
        return HTTPPostEthernetSettings(req, PostData);
    if (!memcmp(filename, url_wifi_settings, sizeof(url_wifi_settings)))
        return HTTPPostWiFiSettings(req, PostData);
    if (!memcmp(filename, url_gprs_settings, sizeof(url_gprs_settings)))
        return HTTPPostGPRSSettings(req, PostData);
    if (!memcmp(filename, url_mqtt_settings, sizeof(url_mqtt_settings)))
        return HTTPPostMQTTSettings(req, PostData);
    if (!memcmp(filename, url_sys_settings, sizeof(url_sys_settings)))
        return HTTPPostSystemSettings(req, PostData);
    if (!memcmp(filename, url_time_settings, sizeof(url_time_settings)))
        return HTTPPostTimeSettings(req, PostData);
    if (!memcmp(filename, url_reboot, sizeof(url_reboot)))
        return HTTPPostReboot(req, PostData);

    // If not found target URL here, try to call custom code
     if (AfterPostHandlerCust != NULL)
     AfterPostHandlerCust(req, filename, PostData);

    return HTTP_IO_DONE;
}

static HTTP_IO_RESULT HTTPPostEthernetSettings(httpd_req_t *req, char *PostData)
{
#if CONFIG_WEBGUIAPP_ETHERNET_ENABLE
    char tmp[32];
    bool TempIsETHEnabled = false;
    bool TempIsDHCPEnabled = false;
    if (httpd_query_key_value(PostData, "ethen", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsETHEnabled = true;
    }
    if (httpd_query_key_value(PostData, "dhcp", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsDHCPEnabled = true;
    }

    if (httpd_query_key_value(PostData, "ipa", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->ethSettings.IPAddr);
    if (httpd_query_key_value(PostData, "mas", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->ethSettings.Mask);
    if (httpd_query_key_value(PostData, "gte", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->ethSettings.Gateway);
    if (httpd_query_key_value(PostData, "dns1", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->ethSettings.DNSAddr1);
    if (httpd_query_key_value(PostData, "dns2", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->ethSettings.DNSAddr2);
    if (httpd_query_key_value(PostData, "dns3", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->ethSettings.DNSAddr3);

    if (httpd_query_key_value(PostData, "sav", tmp, 4) == ESP_OK)
    {
        if (!strcmp(tmp, (const char*) "prs"))
        {
            GetSysConf()->ethSettings.Flags1.bIsETHEnabled = TempIsETHEnabled;
            GetSysConf()->ethSettings.Flags1.bIsDHCPEnabled = TempIsDHCPEnabled;
            WriteNVSSysConfig(GetSysConf());
            memcpy(PostData, "/reboot.html", sizeof "/reboot.html");
            return HTTP_IO_REDIRECT;
        }
    }
#endif
    return HTTP_IO_DONE;
}

static HTTP_IO_RESULT HTTPPostWiFiSettings(httpd_req_t *req, char *PostData)
{
#if CONFIG_WEBGUIAPP_WIFI_ENABLE
    char tmp[32];
    bool TempIsWiFiEnabled = false;
    bool TempIsDHCPEnabled = false;
    if (httpd_query_key_value(PostData, "wifien", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsWiFiEnabled = true;
    }
    if (httpd_query_key_value(PostData, "netm", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            GetSysConf()->wifiSettings.Flags1.bIsAP = true;
        else if (!strcmp((const char*) tmp, (const char*) "2"))
            GetSysConf()->wifiSettings.Flags1.bIsAP = false;
    }
    /*AP section*/
    httpd_query_key_value(PostData, "wfiap", GetSysConf()->wifiSettings.ApSSID,
                          sizeof(GetSysConf()->wifiSettings.ApSSID));
    if (httpd_query_key_value(PostData, "wfpap", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (strcmp(tmp, (const char*) "********"))
            strcpy(GetSysConf()->wifiSettings.ApSecurityKey, tmp);
    }
    if (httpd_query_key_value(PostData, "ipaap", tmp, sizeof(tmp)) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->wifiSettings.ApIPAddr);

    httpd_query_key_value(PostData, "wfi", GetSysConf()->wifiSettings.InfSSID,
                          sizeof(GetSysConf()->wifiSettings.InfSSID));
    if (httpd_query_key_value(PostData, "wfp", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (strcmp(tmp, (const char*) "********"))
            strcpy(GetSysConf()->wifiSettings.InfSecurityKey, tmp);
    }
    /*STATION section*/
    if (httpd_query_key_value(PostData, "dhcp", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsDHCPEnabled = true;
    }
    if (httpd_query_key_value(PostData, "ipa", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->wifiSettings.InfIPAddr);
    if (httpd_query_key_value(PostData, "mas", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->wifiSettings.InfMask);
    if (httpd_query_key_value(PostData, "gte", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->wifiSettings.InfGateway);
    if (httpd_query_key_value(PostData, "dns", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->wifiSettings.DNSAddr1);
    if (httpd_query_key_value(PostData, "dns2", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->wifiSettings.DNSAddr2);
    if (httpd_query_key_value(PostData, "dns3", tmp, 15) == ESP_OK)
        esp_netif_str_to_ip4(tmp, (esp_ip4_addr_t*) &GetSysConf()->wifiSettings.DNSAddr3);

    if (httpd_query_key_value(PostData, "sav", tmp, 4) == ESP_OK)
    {
        if (!strcmp(tmp, (const char*) "prs"))
        {
            GetSysConf()->wifiSettings.Flags1.bIsWiFiEnabled = TempIsWiFiEnabled;
            GetSysConf()->wifiSettings.Flags1.bIsDHCPEnabled = TempIsDHCPEnabled;
            WriteNVSSysConfig(GetSysConf());
            memcpy(PostData, "/reboot.html", sizeof "/reboot.html");
            return HTTP_IO_REDIRECT;
        }
    }
#endif
    return HTTP_IO_DONE;
}

static HTTP_IO_RESULT HTTPPostGPRSSettings(httpd_req_t *req, char *PostData)
{
#if    CONFIG_WEBGUIAPP_GPRS_ENABLE
    char tmp[32];
    bool TempIsGSMEnabled = false;
    if (httpd_query_key_value(PostData, "gsmen", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsGSMEnabled = true;
    }

    if (httpd_query_key_value(PostData, "sav", tmp, 4) == ESP_OK)
    {
        if (!strcmp(tmp, (const char*) "prs"))
        {
            GetSysConf()->gsmSettings.Flags1.bIsGSMEnabled = TempIsGSMEnabled;
            WriteNVSSysConfig(GetSysConf());
            memcpy(PostData, "/reboot.html", sizeof "/reboot.html");
            return HTTP_IO_REDIRECT;
        }
    }
    if (httpd_query_key_value(PostData, "restart", tmp, 4) == ESP_OK)
    {
        if (!strcmp(tmp, (const char*) "prs"))
        {
            //PPPModemSoftRestart();
        }
    }
    if (httpd_query_key_value(PostData, "hdrst", tmp, 4) == ESP_OK)
    {
        if (!strcmp(tmp, (const char*) "prs"))
        {
            //PPPModemColdStart();
        }
    }
#endif
    return HTTP_IO_DONE;
}

static HTTP_IO_RESULT HTTPPostMQTTSettings(httpd_req_t *req, char *PostData)
{
#if CONFIG_WEBGUIAPP_MQTT_ENABLE
    char tmp[33];
    bool TempIsMQTT1Enabled = false;
#if CONFIG_MQTT_CLIENTS_NUM == 2
    bool TempIsMQTT2Enabled = false;
#endif
    httpd_query_key_value(PostData, "cld1", GetSysConf()->mqttStation[0].ServerAddr,
                          sizeof(GetSysConf()->mqttStation[0].ServerAddr));
    httpd_query_key_value(PostData, "idd1", GetSysConf()->mqttStation[0].ClientID,
                          sizeof(GetSysConf()->mqttStation[0].ClientID));
    httpd_query_key_value(PostData, "top1", GetSysConf()->mqttStation[0].RootTopic,
                          sizeof(GetSysConf()->mqttStation[0].RootTopic));
    httpd_query_key_value(PostData, "clnm1", GetSysConf()->mqttStation[0].UserName,
                          sizeof(GetSysConf()->mqttStation[0].UserName));

    if (httpd_query_key_value(PostData, "mqttenb1", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsMQTT1Enabled = true;
    }
    if (httpd_query_key_value(PostData, "mprt1", tmp, sizeof(tmp)) == ESP_OK)
        if (httpd_query_key_value(PostData, "mprt1", tmp, sizeof(tmp)) == ESP_OK)
        {
            uint16_t tp = atoi((const char*) tmp);
            if (tp < 65535 && tp >= 1000)
                GetSysConf()->mqttStation[0].ServerPort = tp;
        }

    if (httpd_query_key_value(PostData, "clps1", tmp, sizeof(tmp)) == ESP_OK &&
            strcmp(tmp, (const char*) "******"))
    {
        strcpy(GetSysConf()->mqttStation[0].UserPass, tmp);
    }

#if CONFIG_MQTT_CLIENTS_NUM == 2
    httpd_query_key_value(PostData, "cld2", GetSysConf()->mqttStation[1].ServerAddr,
                          sizeof(GetSysConf()->mqttStation[1].ServerAddr));
    httpd_query_key_value(PostData, "idd2", GetSysConf()->mqttStation[1].ClientID,
                          sizeof(GetSysConf()->mqttStation[1].ClientID));
    httpd_query_key_value(PostData, "top2", GetSysConf()->mqttStation[1].RootTopic,
                          sizeof(GetSysConf()->mqttStation[1].RootTopic));
    httpd_query_key_value(PostData, "clnm2", GetSysConf()->mqttStation[1].UserName,
                          sizeof(GetSysConf()->mqttStation[1].UserName));


    if (httpd_query_key_value(PostData, "mqttenb2", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsMQTT2Enabled = true;
    }

    if (httpd_query_key_value(PostData, "mprt2", tmp, sizeof(tmp)) == ESP_OK)
        if (httpd_query_key_value(PostData, "mprt2", tmp, sizeof(tmp)) == ESP_OK)
        {
            uint16_t tp = atoi((const char*) tmp);
            if (tp < 65535 && tp >= 1000)
                GetSysConf()->mqttStation[1].ServerPort = tp;
        }

    if (httpd_query_key_value(PostData, "clps2", tmp, sizeof(tmp)) == ESP_OK &&
            strcmp(tmp, (const char*) "******"))
    {
        strcpy(GetSysConf()->mqttStation[1].UserPass, tmp);
    }

#endif

    if (httpd_query_key_value(PostData, "sav", tmp, 4) == ESP_OK)
    {
        if (!strcmp(tmp, (const char*) "prs"))
        {
            GetSysConf()->mqttStation[0].Flags1.bIsGlobalEnabled = TempIsMQTT1Enabled;
#if CONFIG_MQTT_CLIENTS_NUM == 2
            GetSysConf()->mqttStation[1].Flags1.bIsGlobalEnabled = TempIsMQTT2Enabled;
#endif
            WriteNVSSysConfig(GetSysConf());
            memcpy(PostData, "/reboot.html", sizeof "/reboot.html");
            return HTTP_IO_REDIRECT;
        }
    }
#endif
    return HTTP_IO_DONE;
}

static HTTP_IO_RESULT HTTPPostSystemSettings(httpd_req_t *req, char *PostData)
{
    char tmp[64];
    bool TempIsTCPConfirm = false;
    bool TempIsLoRaConfirm = false;
    bool TempIsOTAEnabled = false;

    if (httpd_query_key_value(PostData, "nam", tmp, sizeof(tmp)) == ESP_OK)
    {
        UnencodeURL(tmp);
        strcpy(GetSysConf()->NetBIOSName, tmp);
    }
    if (httpd_query_key_value(PostData, "otaurl", tmp, sizeof(tmp)) == ESP_OK)
    {
        UnencodeURL(tmp);
        strcpy(GetSysConf()->OTAURL, tmp);
    }

    httpd_query_key_value(PostData, "lgn", GetSysConf()->SysName, sizeof(GetSysConf()->SysName));

    if (httpd_query_key_value(PostData, "psn", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (strcmp(tmp, (const char*) "******"))
            strcpy(GetSysConf()->SysPass, tmp);
    }

    if (httpd_query_key_value(PostData, "lrdel", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsLoRaConfirm = true;
    }

    if (httpd_query_key_value(PostData, "tcpdel", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsTCPConfirm = true;
    }

    if (httpd_query_key_value(PostData, "ota", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp((const char*) tmp, (const char*) "1"))
            TempIsOTAEnabled = true;
    }

    if (httpd_query_key_value(PostData, "upd", tmp, sizeof(tmp)) == ESP_OK)
    {

        if (!strcmp(tmp, (const char*) "prs"))
        {
            //StartOTA();
        }

    }
    if (httpd_query_key_value(PostData, "rst", tmp, sizeof(tmp)) == ESP_OK)
    {

        if (!strcmp(tmp, (const char*) "prs"))
        {
            memcpy(PostData, "/reboot.html", sizeof "/reboot.html");
            return HTTP_IO_REDIRECT;
        }
    }
    if (httpd_query_key_value(PostData, "sav", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp(tmp, (const char*) "prs"))
        {
            GetSysConf()->Flags1.bIsTCPConfirm = TempIsTCPConfirm;
            GetSysConf()->Flags1.bIsLoRaConfirm = TempIsLoRaConfirm;
            GetSysConf()->Flags1.bIsOTAEnabled = TempIsOTAEnabled;

            WriteNVSSysConfig(GetSysConf());
            memcpy(PostData, "/reboot.html", sizeof "/reboot.html");
            return HTTP_IO_REDIRECT;
        }
    }
    return HTTP_IO_DONE;
}

static HTTP_IO_RESULT HTTPPostTimeSettings(httpd_req_t *req, char *PostData)
{
    return HTTP_IO_DONE;
}

static HTTP_IO_RESULT HTTPPostReboot(httpd_req_t *req, char *PostData)
{
    char tmp[33];
    if (httpd_query_key_value(PostData, "rbt", tmp, sizeof(tmp)) == ESP_OK)
    {
        if (!strcmp(tmp, (const char*) "prs"))
        {
            DelayedRestart();
        }
    }
    return HTTP_IO_DONE;
}

