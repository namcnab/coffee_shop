package main

import (
	"encoding/json"
	"log"
	"net/http"

	"coffee-shop/models"

	"github.com/gin-gonic/gin"
)

func main() {

	ginEngine := gin.Default()
	ginEngine.GET("/ping", ping)
	ginEngine.POST("/order", placeOrder)
	ginEngine.Run() // listen and serve on 0.0.0.0:8080
}

func ping(ginCtx *gin.Context) {
	ginCtx.JSON(200, gin.H{
		"message": "pong",
	})
}

func placeOrder(ginCtx *gin.Context) {
	if ginCtx.Request.Method != http.MethodPut {
		ginCtx.JSON(405, gin.H{
			"message": "method not allowed",
		})
		return
	} else if ginCtx.Request.Body == nil {
		ginCtx.JSON(400, gin.H{
			"message": "empty request body",
		})
		return
	}

	order := new(models.Order)

	// Parse request body into Order struct
	if err := ginCtx.BindJSON(order).Error(); err != "" {
		log.Println(err)
		ginCtx.JSON(400, gin.H{
			"message": "invalid request body",
		})
		return
	}
	// Convert Order struct into bytes
	orderInBytes, err := json.Marshal(order)
	if err != nil {
		log.Println(err)
		ginCtx.JSON(500, gin.H{
			"message": "internal server error",
		})
		return
	}

	// Send the bytes to the Kafka topic
	err = models.PushOrderToQueue("coffe_order", orderInBytes)

	if err != nil {
		log.Println(err)
		ginCtx.JSON(500, gin.H{
			"message": "internal server error",
		})
		return
	}

	// Respond to the client
	response := map[string]interface{}{
		"success": true,
		"message": "Order for " + order.CustomerName + " has been placed successfully!",
	}
	ginCtx.JSON(200, response)
}
