package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {

	ginEngine := gin.Default()
	ginEngine.GET("/ping", ping)
	ginEngine.POST("/order", takeOrder)
	ginEngine.Run() // listen and serve on 0.0.0.0:8080
}

func ping(ginCtx *gin.Context) {
	ginCtx.JSON(200, gin.H{
		"message": "pong",
	})
}

func takeOrder(ginCtx *gin.Context) {
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

}
