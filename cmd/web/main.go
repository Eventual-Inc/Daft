package main

import (
	"github.com/gin-gonic/gin"
	ginSwagger "github.com/swaggo/gin-swagger" // gin-swagger middleware
	"github.com/swaggo/gin-swagger/swaggerFiles"
)

// @title           Daft Web API
// @version         0.1
// @description     This is a server for the Daft frontend and CLI
// @termsOfService  https://www.eventualcomputing.com/terms

// @contact.name   API Support
// @contact.url    https://www.eventualcomputing.com/support
// @contact.email  support@eventualcomputing.com

// @host      localhost:8888
// @BasePath  /api/v1

// @securityDefinitions.apikey  ApiKeyAuth
// @in                          header
// @name                        Authorization
// @description					Authorizes usage of the Daft API

func main() {
	r := gin.Default()

	v1 := r.Group("/api/v1")
	{
		invocations := v1.Group("/invocations")
		{
			invocations.POST("", InvocationsPostHandler)
		}
	}
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	r.Run(":8080")
}
