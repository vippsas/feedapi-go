package feedapi_service

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
)

var feedApiDiscoveryJsonStr = []byte(
	`{
	  "httpRequest": {
        "method": "GET",
        "path": "/api/v1/feed"
      },
      "httpResponse": {
        "statusCode": 200,
        "body": {
          "token": "1",
          "partitions": [{
            "id": "0"
          }]
        }
      }
    }`)

func SetFeedApiDiscoveryResponse(host string) error {
	return SetExpectation(host, feedApiDiscoveryJsonStr)
}

var dynamicFeedApiPartitionResponseJsonStr = `{
	  "httpRequest": {
        "method": "GET",
        "path": "/api/v1/feed/events",
        "queryStringParameters": {
          "token": ["1"],
          "partition": ["0"],
          "cursor": ["%s"]
        }
      },
      "httpResponse": {
        "statusCode": 200,
        "body": 
           "{\"data\":{\"specVersion\":\"1.0\",\"dataContentType\":\"application/json\",\"type\":\"%s\",\"source\":\"%s\",\"id\":\"%s\",\"subject\":\"%s\",\"time\":\"2023-08-30T14:00:45.050103+00:00\",\"data\":{\"userId\":\"%s\",\"personId\":\"%s\",\"customerId\":\"%s\",\"market\":\"%s\",\"msisdn\":\"%s\",\"email\":\"%s\",\"time\":\"2023-08-30T14:00:45.050103+00:00\"}}}\n{\"cursor\":\"%s\"}"
      }
    }`

func SetFeedApiPartitionResponse(host, cursor, nextCursor, eventType, userId, personId, customerId, market, msisdn, email string) error {
	source := "User"
	id := userId + "/0/" + eventType
	subject := userId
	response := []byte(fmt.Sprintf(dynamicFeedApiPartitionResponseJsonStr, cursor, eventType, source, id, subject, userId, personId, customerId, market, msisdn, email, nextCursor))
	return SetExpectation(host, response)
}

var dynamicFeedApiCursorResponseJsonStr = `{
	  "httpRequest": {
        "method": "GET",
        "path": "/api/v1/feed/events",
        "queryStringParameters": {
          "token": ["1"],
          "partition": ["0"],
          "cursor": ["%s"]
        }
      },
      "httpResponse": {
        "statusCode": 200,
      	"body": "{\"cursor\":\"%s\"}"
      }
    }`

func SetStartAndEndCursorFeedApiResponse(host, startCursor, endCursor string) error {
	response := []byte(fmt.Sprintf(dynamicFeedApiCursorResponseJsonStr, startCursor, endCursor))
	return SetExpectation(host, response)
}

func SetFeedApiCursorResponse(host, cursor string) error {
	response := []byte(fmt.Sprintf(dynamicFeedApiCursorResponseJsonStr, cursor, cursor))
	return SetExpectation(host, response)
}

func SetExpectation(host string, jsonStr []byte) error {
	request, err := http.NewRequest(http.MethodPut, host+"/expectation", bytes.NewBuffer(jsonStr))
	if err != nil {
		return err
	}

	request.Header.Add("Content-type", "application/json")
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return err
	} else if response.StatusCode != http.StatusCreated {
		return errors.New(fmt.Sprintf("Expected status code %d. Got %d.", http.StatusCreated, response.StatusCode))
	}
	return nil
}

func RemoveFeedInfo(host string) error {
	RemoveFeedApiDiscovery := `{
    "httpRequest": {
      "method": "GET",
      "path": "/api/v1/feed"
    }
  }`

	RemoveFeedApiEvents := `{
    "httpRequest": {
      "method": "GET",
      "path": "/api/v1/feed/events"
    }
  }`
	request1, err := http.NewRequest(http.MethodPut, host+"/clear", bytes.NewBuffer([]byte(RemoveFeedApiDiscovery)))
	if err != nil {
		return err
	}
	request2, err := http.NewRequest(http.MethodPut, host+"/clear", bytes.NewBuffer([]byte(RemoveFeedApiEvents)))
	if err != nil {
		return err
	}

	request1.Header.Add("Content-type", "application/json")
	request2.Header.Add("Content-type", "application/json")

	client := &http.Client{}
	response, err := client.Do(request1)
	if err != nil {
		return err
	} else if response.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("Expected status code %d. Got %d.", http.StatusCreated, response.StatusCode))
	}

	response, err = client.Do(request2)
	if err != nil {
		return err
	} else if response.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("Expected status code %d. Got %d.", http.StatusCreated, response.StatusCode))
	}
	return nil
}
