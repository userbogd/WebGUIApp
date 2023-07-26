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
 *  	 \file SysComm.c
 *    \version 1.0
 * 		 \date 2023-07-26
 *     \author Bogdan Pilyugin
 * 	    \brief    
 *    \details 
 *	\copyright Apache License, Version 2.0
 */

/*
 //Example of remote transaction [payloadtype=1]  start[msgtype=1]
 {
 "data":{
 "msgid":123456789,
 "time":"2023-06-03T12:25:24+00:00",
 "msgtype":1,
 "payloadtype":50,
 "payload":{
 "key1":"value1",
 "key2":"value2"}},
 "signature":"6a11b872e8f766673eb82e127b6918a0dc96a42c5c9d184604f9787f3d27bcef"}


 //Example of request [msgtype=2] of information related to transaction [payloadtype=1]
 {
 "data":{
 "msgid":123456789,
 "time":"2023-06-03T12:25:24+00:00",
 "msgtype":2,
 "payloadtype":50},
 "signature":"3c1254d5b0e7ecc7e662dd6397554f02622ef50edba18d0b30ecb5d53e409bcb"}


 //Example of response [msgtype=3] to request related to the transaction [payloadtype=1]
 {
 "data":{
 "msgid":123456789,
 "time":"2023-06-03T12:25:24+00:00",
 "msgtype": 3,
 "payloadtype":50,
 "payload":{
 "key1":"value1",
 "key2":"value2"},
 "error":"SYS_OK",
 "error_descr":"Result successful"},
 "signature":"0d3b545b7c86274a6bf5a6e606b260f32b1999de40cb7d29d0949ecc9389cd9d"}

 */

#include "webguiapp.h"
#include "SystemApplication.h"
#include "mbedtls/md.h"

#define TAG "SysComm"

#define MAX_JSON_DATA_SIZE 1024

static esp_err_t SHA256hmacHash(unsigned char *data,
                                int datalen,
                                unsigned char *key,
                                int keylen,
                                unsigned char *res)
{
    mbedtls_md_context_t ctx;
    mbedtls_md_type_t md_type = MBEDTLS_MD_SHA256;
    mbedtls_md_init(&ctx);
    mbedtls_md_setup(&ctx, mbedtls_md_info_from_type(md_type), 1);
    mbedtls_md_hmac_starts(&ctx, key, keylen);
    mbedtls_md_hmac_update(&ctx, (const unsigned char*) data, datalen);
    mbedtls_md_hmac_finish(&ctx, res);
    mbedtls_md_free(&ctx);
    return ESP_OK;
}

static void Timestamp(char *ts)
{
    struct timeval tp;
    gettimeofday(&tp, NULL);
    unsigned long long ms = (((unsigned long long) tp.tv_sec) * 1000000 + tp.tv_usec);
    sprintf(ts, "%llu", ms);
}

void PrepareResponsePayloadType50(data_message_t *MSG)
{
    const char *err_br;
    const char *err_desc;
    jwOpen(MSG->outputDataBuffer, MSG->outputDataLength, JW_OBJECT, JW_PRETTY);
    jwObj_int("msgid", MSG->parsedData.msgID);
    char time[RFC3339_TIMESTAMP_LENGTH];
    GetRFC3339Time(time);
    jwObj_string("time", time);
    jwObj_int("messtype", DATA_MESSAGE_TYPE_RESPONSE);
    jwObj_int("payloadtype", 50);
    //PAYLOAD BEGIN
    jwObj_object("payload");

    jwObj_string("param1", "value1");
    jwObj_string("param2", "value2");

    jwEnd();
    GetSysErrorDetales((sys_error_code) MSG->err_code, &err_br, &err_desc);
    jwObj_string("error", (char*) err_br);
    jwObj_string("error_descr", (char*) err_desc);
    jwEnd();
    jwClose();
}

static sys_error_code SysPayloadType50Handler(data_message_t *MSG)
{
    struct jReadElement result;
    payload_type_50 *payload;
    payload = ((payload_type_50*) (MSG->parsedData.payload));
    if (MSG->parsedData.msgType == DATA_MESSAGE_TYPE_COMMAND)
    {
        //Extract 'key1' or throw exception
        jRead(MSG->inputDataBuffer, "{'data'{'payload'{'key1'", &result);
        if (result.elements == 1)
        {

        }
        else
            return SYS_ERROR_PARSE_KEY1;

        //Extract 'key1' or throw exception
        jRead(MSG->inputDataBuffer, "{'data'{'payload'{'key2'", &result);
        if (result.elements == 1)
        {

        }
        else
            return SYS_ERROR_PARSE_KEY2;
        //return StartTransactionPayloadType1(MSG);

    }

    else if (MSG->parsedData.msgType == DATA_MESSAGE_TYPE_REQUEST)
    {
        PrepareResponsePayloadType50(MSG);
        return SYS_OK_DATA;
    }
    else
        return SYS_ERROR_PARSE_MSGTYPE;

    return SYS_OK;
}

static sys_error_code SysDataParser(data_message_t *MSG)
{
    struct jReadElement result;
    jRead(MSG->inputDataBuffer, "", &result);
    if (result.dataType != JREAD_OBJECT)
        return SYS_ERROR_WRONG_JSON_FORMAT;
    MSG->parsedData.msgID = 0;

    char *hashbuf = malloc(MSG->inputDataLength);
    if (hashbuf == NULL)
        return SYS_ERROR_NO_MEMORY;
    jRead_string(MSG->inputDataBuffer, "{'data'", hashbuf, MSG->inputDataLength, 0);
    if (strlen(hashbuf) > 0)
    {
        SHA256hmacHash((unsigned char*) hashbuf, strlen(hashbuf), (unsigned char*) "mykey", sizeof("mykey"),
                       MSG->parsedData.sha256);
        unsigned char sha_print[32 * 2 + 1];
        BytesToStr(MSG->parsedData.sha256, sha_print, 32);
        sha_print[32 * 2] = 0x00;
        ESP_LOGI(TAG, "SHA256 of DATA object is %s", sha_print);
        free(hashbuf);
    }
    else
    {
        free(hashbuf);
        return SYS_ERROR_PARSE_DATA;
    }

    jRead(MSG->inputDataBuffer, "{'signature'", &result);
    if (result.elements == 1)
    {
        ESP_LOGI(TAG, "Signature is %.*s", 64, (char* )result.pValue);
        //Here compare calculated and received signature;
    }
    else
        return SYS_ERROR_PARSE_SIGNATURE;

    //Extract 'messidx' or throw exception
    jRead(MSG->inputDataBuffer, "{'data'{'msgid'", &result);
    if (result.elements == 1)
    {
        MSG->parsedData.msgID = jRead_long(MSG->inputDataBuffer, "{'data'{'msgid'", 0);
        if (MSG->parsedData.msgID == 0)
            return SYS_ERROR_PARSE_MESSAGEID;
    }
    else
        return SYS_ERROR_PARSE_MESSAGEID;

    //Extract 'msgtype' or throw exception
    jRead(MSG->inputDataBuffer, "{'data'{'msgtype'", &result);
    if (result.elements == 1)
    {
        MSG->parsedData.msgType = atoi((char*) result.pValue);
        if (MSG->parsedData.msgType > 2 || MSG->parsedData.msgType < 1)
            return SYS_ERROR_PARSE_MSGTYPE;
    }
    else
        return SYS_ERROR_PARSE_MSGTYPE;

    //Extract 'payloadtype' or throw exception
    jRead(MSG->inputDataBuffer, "{'data'{'payloadtype'", &result);
    if (result.elements == 1)
    {
        MSG->parsedData.payloadType = atoi((char*) result.pValue);
        if (MSG->parsedData.payloadType < 1 && MSG->parsedData.payloadType > 100)
            return SYS_ERROR_PARSE_PAYLOADTYPE;
        MSG->parsedData.payload = malloc(sizeof(payload_type_50));
    }
    else
        return SYS_ERROR_PARSE_PAYLOADTYPE;

    switch (MSG->parsedData.payloadType)
    {
        case 50:
            return SysPayloadType50Handler(MSG);
        break;
    }

    return SYS_ERROR_UNKNOWN;
}

esp_err_t SysServiceDataHandler(data_message_t *MSG)
{
    MSG->err_code = (int) SysDataParser(MSG);
    if (MSG->err_code)
    {
        jwOpen(MSG->outputDataBuffer, MSG->outputDataLength, JW_OBJECT, JW_PRETTY);
        jwObj_int("msgid", MSG->parsedData.msgID);
        char time[RFC3339_TIMESTAMP_LENGTH];
        GetRFC3339Time(time);
        jwObj_string("time", time);
        jwObj_int("messtype", DATA_MESSAGE_TYPE_RESPONSE);
        const char *err_br;
        const char *err_desc;
        GetSysErrorDetales((sys_error_code) MSG->err_code, &err_br, &err_desc);
        jwObj_string("error", (char*) err_br);
        jwObj_string("error_descr", (char*) err_desc);
        jwEnd();
        jwClose();
    }

    return ESP_OK;
}