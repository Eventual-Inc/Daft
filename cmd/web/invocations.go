package main

import (
	"fmt"
	"net/http"

	"github.com/Eventual-Inc/Daft/cmd/web/errors"
	"github.com/Eventual-Inc/Daft/cmd/web/model"
	"github.com/gin-gonic/gin"
)

// ShowAccount godoc
// @Summary      Create a function invocation
// @Description  Invokes a function (creating an Invocation) using a set of inputs
// @Tags         invocations
// @Accept       json
// @Produce      json
// @Success      200  {object}  model.CreateInvocationSuccess
// @Failure      400  {object}  errors.HTTPError
// @Router       /invocations [post]
func InvocationsPostHandler(ctx *gin.Context) {
	var createInvocationPayload model.CreateInvocation
	if err := ctx.ShouldBindJSON(&createInvocationPayload); err != nil {
		errors.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	fmt.Println(createInvocationPayload)
	success := model.CreateInvocationSuccess{ID: createInvocationPayload.ID}
	ctx.JSON(http.StatusOK, success)
}
